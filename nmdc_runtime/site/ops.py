import csv
import json
import mimetypes
import os
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO, StringIO
from toolz.dicttoolz import keyfilter
from typing import Tuple
from zipfile import ZipFile
from itertools import chain

import pandas as pd
import requests


from bson import ObjectId, json_util
from dagster import (
    Any,
    AssetKey,
    AssetMaterialization,
    Dict,
    Failure,
    List,
    MetadataValue,
    OpExecutionContext,
    Out,
    Output,
    RetryPolicy,
    RetryRequested,
    String,
    op,
    Optional,
    Field,
    Permissive,
    Bool,
)
from gridfs import GridFS
from linkml_runtime.dumpers import json_dumper
from linkml_runtime.utils.yamlutils import YAMLRoot
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.metadata import (
    _validate_changesheet,
    df_from_sheet_in,
    get_collection_for_id,
    map_id_to_collection,
)
from nmdc_runtime.api.core.util import dotted_path_for, hash_from_str, json_clean, now
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.endpoints.find import find_study_by_id
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.operation import (
    ObjectPutMetadata,
    Operation,
    UpdateOperationRequest,
)
from nmdc_runtime.api.models.run import (
    RunEventType,
    RunSummary,
    _add_run_complete_event,
)
from nmdc_runtime.api.models.util import ResultT
from nmdc_runtime.site.export.ncbi_xml import NCBISubmissionXML
from nmdc_runtime.site.export.ncbi_xml_utils import (
    fetch_data_objects_from_biosamples,
    fetch_nucleotide_sequencing_from_biosamples,
    fetch_library_preparation_from_biosamples,
    get_instruments,
)
from nmdc_runtime.site.drsobjects.ingest import mongo_add_docs_result_as_dict
from nmdc_runtime.site.resources import (
    NmdcPortalApiClient,
    GoldApiClient,
    RuntimeApiSiteClient,
    RuntimeApiUserClient,
    NeonApiClient,
    MongoDB as MongoDBResource,
)
from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator
from nmdc_runtime.site.translation.neon_soil_translator import NeonSoilDataTranslator
from nmdc_runtime.site.translation.neon_benthic_translator import (
    NeonBenthicDataTranslator,
)
from nmdc_runtime.site.translation.neon_surface_water_translator import (
    NeonSurfaceWaterDataTranslator,
)
from nmdc_runtime.site.translation.submission_portal_translator import (
    SubmissionPortalTranslator,
)
from nmdc_runtime.site.repair.database_updater import DatabaseUpdater
from nmdc_runtime.site.util import (
    run_and_log,
    schema_collection_has_index_on_id,
    nmdc_study_id_to_filename,
)
from nmdc_runtime.util import (
    drs_object_in_for,
    get_names_of_classes_in_effective_range_of_slot,
    pluralize,
    put_object,
    validate_json,
    specialize_activity_set_docs,
    collection_name_to_class_names,
    class_hierarchy_as_list,
    nmdc_schema_view,
    populated_schema_collection_names_with_id_field,
)
from nmdc_schema import nmdc
from nmdc_schema.nmdc import Database as NMDCDatabase
from pydantic import BaseModel
from pymongo import InsertOne
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import assoc, dissoc, get_in, valfilter, identity


@op
def hello(context):
    """
    A solid definition. This example solid outputs a single string.

    For more hints about writing Dagster solids, see our documentation overview on Solids:
    https://docs.dagster.io/overview/solids-pipelines/solids
    """
    name = context.op_config.get("name", "NMDC") if context.op_config else "NMDC"
    out = f"Hello, {name}!"
    context.log.info(out)
    return out


@op
def log_env(context):
    env = subprocess.check_output("printenv", shell=True).decode()
    out = [line for line in env.splitlines() if line.startswith("DAGSTER_")]
    context.log.info("\n".join(out))


@op(required_resource_keys={"mongo"})
def mongo_stats(context) -> List[str]:
    db = context.resources.mongo.db
    collection_names = db.list_collection_names()
    context.log.info(str(collection_names))
    return collection_names


@op(
    required_resource_keys={"mongo", "runtime_api_site_client"},
    retry_policy=RetryPolicy(max_retries=2),
)
def local_file_to_api_object(context, file_info):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    storage_path: str = file_info["storage_path"]
    mime_type = file_info.get("mime_type")
    if mime_type is None:
        mime_type = mimetypes.guess_type(storage_path)[0]
    rv = client.put_object_in_site(
        {"mime_type": mime_type, "name": storage_path.rpartition("/")[-1]}
    )
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description=f"put_object_in_site failed: {rv.content}")
    op = rv.json()
    context.log.info(f"put_object_in_site: {op}")
    rv = put_object(storage_path, op["metadata"]["url"])
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description=f"put_object failed: {rv.content}")
    op_patch = {"done": True, "result": drs_object_in_for(storage_path, op)}
    rv = client.update_operation(op["id"], op_patch)
    if not rv.status_code == status.HTTP_200_OK:
        raise Failure(description="update_operation failed")
    op = rv.json()
    context.log.info(f"update_operation: {op}")
    rv = client.create_object_from_op(op)
    if rv.status_code != status.HTTP_201_CREATED:
        raise Failure("create_object_from_op failed")
    obj = rv.json()
    context.log.info(f'Created /objects/{obj["id"]}')
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_one({"id": op["id"]})
    if rv.deleted_count != 1:
        context.log.error("deleting op failed")
    yield AssetMaterialization(
        asset_key=AssetKey(["object", obj["name"]]),
        description="output of metadata-translation run_etl",
        metadata={"object_id": MetadataValue.text(obj["id"])},
    )
    yield Output(obj)


@op(
    out={
        "merged_data_path": Out(
            str,
            description="path to TSV merging of source metadata",
        )
    }
)
def build_merged_db(context) -> str:
    context.log.info("metadata-translation: running `make build-merged-db`")
    run_and_log(
        "cd /opt/dagster/lib/metadata-translation/ && make build-merged-db", context
    )
    storage_path = (
        "/opt/dagster/lib/metadata-translation/src/data/nmdc_merged_data.tsv.zip"
    )
    yield AssetMaterialization(
        asset_key=AssetKey(["gold_translation", "merged_data.tsv.zip"]),
        description="input to metadata-translation run_etl",
        metadata={"path": MetadataValue.path(storage_path)},
    )
    yield Output(storage_path, "merged_data_path")


@op(
    required_resource_keys={"runtime_api_site_client"},
)
def run_etl(context, merged_data_path: str):
    context.log.info("metadata-translation: running `make run-etl`")
    if not os.path.exists(merged_data_path):
        raise Failure(description=f"merged_db not present at {merged_data_path}")
    run_and_log("cd /opt/dagster/lib/metadata-translation/ && make run-etl", context)
    storage_path = (
        "/opt/dagster/lib/metadata-translation/src/data/nmdc_database.json.zip"
    )
    with ZipFile(storage_path) as zf:
        name = zf.namelist()[0]
        with zf.open(name) as f:
            rv = json.load(f)
    context.log.info(f"nmdc_database.json keys: {list(rv.keys())}")
    yield AssetMaterialization(
        asset_key=AssetKey(["gold_translation", "database.json.zip"]),
        description="output of metadata-translation run_etl",
        metadata={
            "path": MetadataValue.path(storage_path),
        },
    )
    yield Output({"storage_path": storage_path})


@op(required_resource_keys={"mongo"})
def get_operation(context):
    mdb = context.resources.mongo.db
    id_op = context.op_config.get("operation_id")
    doc = mdb.operations.find_one({"id": id_op})
    if doc is None:
        raise Failure(description=f"operation {id_op} not found")
    context.log.info(f"got operation {id_op}")
    return Operation(**doc)


@op(
    required_resource_keys={"runtime_api_site_client", "mongo"},
    retry_policy=RetryPolicy(max_retries=2),
)
def produce_curated_db(context, op: Operation):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    mdb: MongoDatabase = context.resources.mongo.db
    op = Operation[ResultT, JobOperationMetadata](**op.model_dump())
    op_meta: JobOperationMetadata = op.metadata
    job_id = op_meta.job.id
    job = mdb.jobs.find_one({"id": job_id})
    o_id = get_in(["config", "object_id_latest"], job)
    rv = client.get_object_bytes(o_id)

    with ZipFile(BytesIO(rv.content)) as myzip:
        name = next(n for n in myzip.namelist() if n.endswith("nmdc_database.json"))
        with myzip.open(name) as f:
            nmdc_database = json.load(f)

    context.log.info(f"{list(nmdc_database.keys())}")
    # TODO do the curation. :)
    return nmdc_database


@op(required_resource_keys={"runtime_api_site_client"})
def create_objects_from_ops(context, op_docs: list):
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    responses = [client.create_object_from_op(doc) for doc in op_docs]
    if {r.status_code for r in responses} == {201}:
        context.log.info("All OK")
    elif responses:
        raise Failure(f"Unexpected response(s): {[r.text for r in responses]}")
    return op_docs


@op(required_resource_keys={"runtime_api_user_client"})
def validate_metadata(context: OpExecutionContext, database: nmdc.Database):
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.validate_metadata(database)
    body = response.json()
    if body["result"] != "All Okay!":
        raise Failure(
            description="Metadata did not validate",
            metadata={"detail": body["detail"]},
        )
    return body


@op(required_resource_keys={"runtime_api_user_client"})
def submit_metadata_to_db(context: OpExecutionContext, database: nmdc.Database) -> str:
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.submit_metadata(database)
    body = response.json()
    return body["detail"]["run_id"]


@op(required_resource_keys={"runtime_api_user_client"})
def poll_for_run_completion(context: OpExecutionContext, run_id: str) -> RunSummary:
    client: RuntimeApiUserClient = context.resources.runtime_api_user_client
    response = client.get_run_info(run_id)
    body = RunSummary.parse_obj(response.json())
    context.log.info(body.status)
    if body.status != RunEventType.COMPLETE:
        raise RetryRequested(max_retries=12, seconds_to_wait=10)
    return body


@op
def filter_ops_done_object_puts() -> str:
    return json_util.dumps(
        {
            "done": True,
            "metadata.model": dotted_path_for(ObjectPutMetadata),
        }
    )


@op
def filter_ops_undone_expired() -> str:
    return json_util.dumps(
        {
            "done": {"$ne": True},
            "expire_time": {"$lt": datetime.now(timezone.utc)},
        }
    )


@op(required_resource_keys={"runtime_api_site_client"})
def list_operations(context, filter_: str) -> list:
    client = context.resources.runtime_api_site_client
    ops = [op.model_dump() for op in client.list_operations({"filter": filter_})]
    context.log.info(str(len(ops)))
    return ops


@op(required_resource_keys={"mongo"})
def delete_operations(context, op_docs: list):
    mdb = context.resources.mongo.db
    rv = mdb.operations.delete_many({"id": {"$in": [doc["id"] for doc in op_docs]}})
    context.log.info(f"Deleted {rv.deleted_count} of {len(op_docs)}")
    if rv.deleted_count != len(op_docs):
        context.log.error("Didn't delete all.")


@op(required_resource_keys={"mongo"})
def construct_jobs(context: OpExecutionContext) -> List[Job]:
    mdb: MongoDatabase = context.resources.mongo.db
    docs = [
        dict(**base, id=generate_one_id(mdb, "jobs"), created_at=now())
        for base in context.op_config["base_jobs"]
    ]
    return [Job(**d) for d in docs]


@op(required_resource_keys={"mongo"})
def maybe_post_jobs(context, jobs: List[Job]):
    mdb: MongoDatabase = context.resources.mongo.db
    n_posted = 0
    for job in jobs:
        job_docs = list(mdb.jobs.find({"workflow.id": job.workflow.id}))
        posted_job_object_ids = [get_in(["config", "object_id"], d) for d in job_docs]
        job_object_id = job.config.get("object_id")
        if job_object_id in posted_job_object_ids:
            context.log.info(
                f"{job.workflow.id} job for object id {job_object_id} already posted"
            )
            continue

        object_id_timestamps = {
            d["id"]: d["created_time"]
            for d in mdb.objects.find(
                {"id": {"$in": posted_job_object_ids + [job_object_id]}},
                ["id", "created_time"],
            )
        }
        candidate_job_object_id_timestamp = object_id_timestamps[job_object_id]
        for id_, ts in object_id_timestamps.items():
            if ts > candidate_job_object_id_timestamp:
                context.log.info(
                    f"{job.workflow.id} job already posted for object id {id_} "
                    f"created later than {job_object_id}"
                )
                break

        mdb.jobs.insert_one(json_clean(job, model=Job, exclude_unset=True))
        yield AssetMaterialization(
            asset_key=AssetKey(["job", job.workflow.id]),
            description="workflow job",
            metadata={
                "object_id": MetadataValue.text(job_object_id),
            },
        )
        n_posted += 1
    context.log.info(f'{n_posted} {pluralize("job", n_posted)}')
    yield Output(n_posted)


@op(required_resource_keys={"mongo"})
def remove_unclaimed_obsolete_jobs(context, job: Job):
    mdb: MongoDatabase = context.resources.mongo.db
    job_object_id = job.config.get("object_id_latest")
    other_job_docs = list(
        mdb.jobs.find(
            {
                "workflow.id": job.workflow.id,
                "config.object_id_latest": {"$ne": job_object_id},
            }
        )
    )
    print(other_job_docs)
    # TODO which of other_job_docs are unclaimed? (no operations)? Delete them.


@op(required_resource_keys={"mongo"})
def get_changesheet_in(context) -> ChangesheetIn:
    mdb: MongoDatabase = context.resources.mongo.db
    object_id = context.op_config.get("object_id")
    mdb_fs = GridFS(mdb)
    grid_out = mdb_fs.get(object_id)
    return ChangesheetIn(
        name=grid_out.filename, content_type=grid_out.content_type, text=grid_out.read()
    )


@op(required_resource_keys={"mongo"})
def perform_changesheet_updates(context, sheet_in: ChangesheetIn):
    mdb: MongoDatabase = context.resources.mongo.db
    op_id = context.op_config.get("operation_id")
    try:
        df_change = df_from_sheet_in(sheet_in, mdb)
        validation_result = _validate_changesheet(df_change, mdb)
    except Exception as e:
        raise Failure(str(e))

    update_cmd = validation_result["update_cmd"]
    results_of_updates = validation_result["results_of_updates"]

    id_dict = map_id_to_collection(mdb)
    docs_to_upsert = defaultdict(list)
    for r in results_of_updates:
        collection_name = get_collection_for_id(r["id"], id_dict)
        docs_to_upsert[collection_name].append(r["doc_after"])
    context.resources.mongo.add_docs(docs_to_upsert)
    op = Operation(**mdb.operations.find_one({"id": op_id}))
    op.done = True
    op.result = {"update_cmd": json.dumps(update_cmd)}
    op_doc = op.model_dump(exclude_unset=True)
    mdb.operations.replace_one({"id": op_id}, op_doc)
    return ["/operations/" + op_doc["id"]]


@op(required_resource_keys={"runtime_api_site_client"})
def get_json_in(context):
    object_id = context.op_config.get("object_id")
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    rv = client.get_object_bytes(object_id)
    if rv.status_code != 200:
        raise Failure(
            description=f"error code {rv.status_code} for {rv.request.url}: {rv.text}"
        )
    return rv.json()


def ensure_data_object_type(docs: Dict[str, list], mdb: MongoDatabase):
    """Does not ensure ordering of `docs`."""

    if ("data_object_set" not in docs) or len(docs["data_object_set"]) == 0:
        return docs, 0

    do_docs = docs["data_object_set"]

    class FileTypeEnumBase(BaseModel):
        name: str
        description: str
        filter: str  # JSON-encoded data_object_set mongo collection filter document

    class FileTypeEnum(FileTypeEnumBase):
        id: str

    temp_collection_name = f"tmp.data_object_set.{ObjectId()}"
    temp_collection = mdb[temp_collection_name]
    temp_collection.insert_many(do_docs)
    temp_collection.create_index("id")

    def fte_matches(fte_filter: str):
        return [
            dissoc(d, "_id") for d in mdb.temp_collection.find(json.loads(fte_filter))
        ]

    do_docs_map = {d["id"]: d for d in do_docs}

    n_docs_with_types_added = 0

    for fte_doc in mdb.file_type_enum.find():
        fte = FileTypeEnum(**fte_doc)
        docs_matching = fte_matches(fte.filter)
        for doc in docs_matching:
            if "data_object_type" not in doc:
                do_docs_map[doc["id"]] = assoc(doc, "data_object_type", fte.id)
                n_docs_with_types_added += 1

    mdb.drop_collection(temp_collection_name)
    return (
        assoc(
            docs, "data_object_set", [dissoc(v, "_id") for v in do_docs_map.values()]
        ),
        n_docs_with_types_added,
    )


@op(required_resource_keys={"runtime_api_site_client", "mongo"})
def perform_mongo_updates(context, json_in):
    mongo = context.resources.mongo
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client
    op_id = context.op_config.get("operation_id")

    docs = json_in
    docs, _ = specialize_activity_set_docs(docs)
    docs, n_docs_with_types_added = ensure_data_object_type(docs, mongo.db)
    context.log.info(f"added `data_object_type` to {n_docs_with_types_added} docs")
    context.log.debug(f"{docs}")

    rv = validate_json(
        docs, mongo.db
    )  # use *exact* same check as /metadata/json:validate
    if rv["result"] == "errors":
        raise Failure(str(rv["detail"]))

    # TODO containing op `perform_mongo_updates` needs test coverage, as below line had trivial bug.
    #   ref: https://github.com/microbiomedata/nmdc-runtime/issues/631
    add_docs_result = _add_schema_docs_with_or_without_replacement(mongo, docs)
    op_patch = UpdateOperationRequest(
        done=True,
        result=add_docs_result,
        metadata={"done_at": datetime.now(timezone.utc).isoformat(timespec="seconds")},
    )
    op_doc = client.update_operation(op_id, op_patch).json()
    return ["/operations/" + op_doc["id"]]


def _add_schema_docs_with_or_without_replacement(
    mongo: MongoDBResource, docs: Dict[str, list]
):
    coll_index_on_id_map = schema_collection_has_index_on_id(mongo.db)
    if all(coll_index_on_id_map[coll] for coll in docs.keys()):
        replace = True
    elif all(not coll_index_on_id_map[coll] for coll in docs.keys()):
        # FIXME: XXX: This is a hack because e.g. <https://w3id.org/nmdc/FunctionalAnnotationAggMember>
        # documents should be unique with compound key (metagenome_annotation_id, gene_function_id)
        # and yet this is not explicit in the schema. One potential solution is to auto-generate an `id`
        # as a deterministic hash of the compound key.
        #
        # For now, decision is to potentially re-insert "duplicate" documents, i.e. to interpret
        # lack of `id` as lack of unique document identity for de-duplication.
        replace = False  # wasting time trying to upsert by `id`.
    else:
        colls_not_id_indexed = [
            coll for coll in docs.keys() if not coll_index_on_id_map[coll]
        ]
        colls_id_indexed = [coll for coll in docs.keys() if coll_index_on_id_map[coll]]
        raise Failure(
            "Simultaneous addition of non-`id`ed collections and `id`-ed collections"
            " is not supported at this time."
            f"{colls_not_id_indexed=} ; {colls_id_indexed=}"
        )
    op_result = mongo.add_docs(docs, validate=False, replace=replace)
    return mongo_add_docs_result_as_dict(op_result)


@op(required_resource_keys={"mongo"})
def add_output_run_event(context: OpExecutionContext, outputs: List[str]):
    mdb = context.resources.mongo.db
    run_event_doc = mdb.run_events.find_one(
        {"run.facets.nmdcRuntime_dagsterRunId": context.run_id}
    )
    if run_event_doc:
        nmdc_run_id = run_event_doc["run"]["id"]
        return _add_run_complete_event(run_id=nmdc_run_id, mdb=mdb, outputs=outputs)
    else:
        context.log.info(f"No NMDC RunEvent doc for Dagster Run {context.run_id}")


@op(
    config_schema={
        "study_id": str,
        "study_type": str,
        "gold_nmdc_instrument_mapping_file_url": str,
        "include_field_site_info": bool,
    },
    out={
        "study_id": Out(str),
        "study_type": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
        "include_field_site_info": Out(bool),
    },
)
def get_gold_study_pipeline_inputs(
    context: OpExecutionContext,
) -> Tuple[str, str, str, bool]:
    return (
        context.op_config["study_id"],
        context.op_config["study_type"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
        context.op_config["include_field_site_info"],
    )


@op(required_resource_keys={"gold_api_client"})
def gold_biosamples_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_biosamples_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_analysis_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> List[Dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_analysis_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_study(context: OpExecutionContext, study_id: str) -> Dict[str, Any]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_study(study_id)


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_gold_study(
    context: OpExecutionContext,
    study: Dict[str, Any],
    study_type: str,
    projects: List[Dict[str, Any]],
    biosamples: List[Dict[str, Any]],
    analysis_projects: List[Dict[str, Any]],
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = GoldStudyTranslator(
        study,
        study_type,
        biosamples,
        projects,
        analysis_projects,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        id_minter=id_minter,
    )
    database = translator.get_database()
    return database


@op(
    out={
        "submission_id": Out(),
        "nucleotide_sequencing_mapping_file_url": Out(Optional[str]),
        "data_object_mapping_file_url": Out(Optional[str]),
        "biosample_extras_file_url": Out(Optional[str]),
        "biosample_extras_slot_mapping_file_url": Out(Optional[str]),
    },
)
def get_submission_portal_pipeline_inputs(
    submission_id: str,
    nucleotide_sequencing_mapping_file_url: Optional[str],
    data_object_mapping_file_url: Optional[str],
    biosample_extras_file_url: Optional[str],
    biosample_extras_slot_mapping_file_url: Optional[str],
) -> Tuple[str, str | None, str | None, str | None, str | None]:
    return (
        submission_id,
        nucleotide_sequencing_mapping_file_url,
        data_object_mapping_file_url,
        biosample_extras_file_url,
        biosample_extras_slot_mapping_file_url,
    )


@op(
    required_resource_keys={"nmdc_portal_api_client"},
)
def fetch_nmdc_portal_submission_by_id(
    context: OpExecutionContext, submission_id: str
) -> Dict[str, Any]:
    client: NmdcPortalApiClient = context.resources.nmdc_portal_api_client
    return client.fetch_metadata_submission(submission_id)


@op(required_resource_keys={"runtime_api_site_client"})
def translate_portal_submission_to_nmdc_schema_database(
    context: OpExecutionContext,
    metadata_submission: Dict[str, Any],
    nucleotide_sequencing_mapping: List,
    data_object_mapping: List,
    study_category: Optional[str],
    study_doi_category: Optional[str],
    study_doi_provider: Optional[str],
    study_pi_image_url: Optional[str],
    biosample_extras: Optional[list[dict]],
    biosample_extras_slot_mapping: Optional[list[dict]],
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = SubmissionPortalTranslator(
        metadata_submission,
        nucleotide_sequencing_mapping=nucleotide_sequencing_mapping,
        data_object_mapping=data_object_mapping,
        id_minter=id_minter,
        study_category=study_category,
        study_doi_category=study_doi_category,
        study_doi_provider=study_doi_provider,
        study_pi_image_url=study_pi_image_url,
        biosample_extras=biosample_extras,
        biosample_extras_slot_mapping=biosample_extras_slot_mapping,
    )
    database = translator.get_database()
    return database


@op
def nmdc_schema_database_export_filename(study: Dict[str, Any]) -> str:
    source_id = None
    if "id" in study:
        source_id = study["id"]
    elif "studyGoldId" in study:
        source_id = study["studyGoldId"]
    return f"database_from_{source_id}.json"


@op
def nmdc_schema_object_to_dict(object: YAMLRoot) -> Dict[str, Any]:
    return json_dumper.to_dict(object)


@op(required_resource_keys={"mongo"}, config_schema={"username": str})
def export_json_to_drs(
    context: OpExecutionContext, data: Dict, filename: str, description: str = ""
) -> List[str]:
    mdb = context.resources.mongo.db
    username = context.op_config.get("username")
    content = json.dumps(data)
    sha256hash = hash_from_str(content, "sha256")
    drs_object = mdb.objects.find_one(
        {"checksums": {"$elemMatch": {"type": "sha256", "checksum": sha256hash}}}
    )
    if drs_object is None:
        drs_object = persist_content_and_get_drs_object(
            content=content,
            username=username,
            filename=filename,
            content_type="application/json",
            description=description,
            id_ns="export-json",
        )
    context.log_event(
        AssetMaterialization(
            asset_key=filename,
            description=description,
            metadata={
                "drs_object_id": MetadataValue.text(drs_object["id"]),
                "json": MetadataValue.json(data),
            },
        )
    )
    return ["/objects/" + drs_object["id"]]


@op(
    description="NCBI Submission XML file rendered in a Dagster Asset",
    out=Out(description="XML content rendered through Dagit UI"),
)
def ncbi_submission_xml_asset(context: OpExecutionContext, data: str):
    filename = "ncbi_submission.xml"
    file_path = os.path.join(context.instance.storage_directory(), filename)

    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "w") as f:
        f.write(data)

    context.log_event(
        AssetMaterialization(
            asset_key="ncbi_submission_xml",
            description="NCBI Submission XML Data",
            metadata={
                "file_path": MetadataValue.path(file_path),
                "xml": MetadataValue.text(data),
            },
        )
    )

    return Output(data)


def unique_field_values(docs: List[Dict[str, Any]], field: str):
    return {doc[field] for doc in docs if field in doc}


@op(config_schema={"mms_data_product": dict})
def get_neon_pipeline_mms_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["mms_data_product"]


@op(config_schema={"sls_data_product": dict})
def get_neon_pipeline_sls_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["sls_data_product"]


@op(config_schema={"benthic_data_product": dict})
def get_neon_pipeline_benthic_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["benthic_data_product"]


@op(config_schema={"surface_water_data_product": dict})
def get_neon_pipeline_surface_water_data_product(context: OpExecutionContext) -> dict:
    return context.op_config["surface_water_data_product"]


@op(required_resource_keys={"neon_api_client"})
def neon_data_by_product(
    context: OpExecutionContext, data_product: dict
) -> Dict[str, pd.DataFrame]:
    df_dict = {}
    client: NeonApiClient = context.resources.neon_api_client

    product_id = data_product["product_id"]
    product_tables = data_product["product_tables"]

    product_table_list = [t.strip() for t in product_tables.split(",")]
    product = client.fetch_product_by_id(product_id)
    for table_name in product_table_list:
        df = pd.DataFrame()
        for site in product["data"]["siteCodes"]:
            for data_url in site["availableDataUrls"]:
                data_files = client.request(data_url)
                for file in data_files["data"]["files"]:
                    if table_name in file["name"] and "expanded" in file["name"]:
                        current_df = pd.read_csv(file["url"])
                        df = pd.concat([df, current_df], ignore_index=True)
        df_dict[table_name] = df

    return df_dict


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_soil_data(
    context: OpExecutionContext,
    mms_data: Dict[str, pd.DataFrame],
    sls_data: Dict[str, pd.DataFrame],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonSoilDataTranslator(
        mms_data,
        sls_data,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_benthic_data(
    context: OpExecutionContext,
    benthic_data: Dict[str, pd.DataFrame],
    site_code_mapping: Dict[str, str],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonBenthicDataTranslator(
        benthic_data,
        site_code_mapping,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_neon_surface_water_data(
    context: OpExecutionContext,
    surface_water_data: Dict[str, pd.DataFrame],
    site_code_mapping: Dict[str, str],
    neon_envo_mappings_file: pd.DataFrame,
    neon_raw_data_file_mappings_file: pd.DataFrame,
    neon_nmdc_instrument_mapping_file: pd.DataFrame,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = NeonSurfaceWaterDataTranslator(
        surface_water_data,
        site_code_mapping,
        neon_envo_mappings_file,
        neon_raw_data_file_mappings_file,
        neon_nmdc_instrument_mapping_file,
        id_minter=id_minter,
    )

    database = translator.get_database()
    return database


@op(
    out={
        "neon_envo_mappings_file_url": Out(),
        "neon_raw_data_file_mappings_file_url": Out(),
        "neon_nmdc_instrument_mapping_file_url": Out(),
    }
)
def get_neon_pipeline_inputs(
    neon_envo_mappings_file_url: str,
    neon_raw_data_file_mappings_file_url: str,
    neon_nmdc_instrument_mapping_file_url: str,
) -> Tuple[str, str, str]:
    return (
        neon_envo_mappings_file_url,
        neon_raw_data_file_mappings_file_url,
        neon_nmdc_instrument_mapping_file_url,
    )


@op
def nmdc_schema_database_export_filename_neon() -> str:
    return "database_from_neon_metadata.json"


@op
def get_csv_rows_from_url(url: Optional[str]) -> List[Dict]:
    """Download and parse a CSV file from a remote URL.

    This method fetches data from the given URL and parses that data as CSV. The parsed data
    is returned as a list (each element corresponds to a row) of dicts (each key is a column
    name and the value is the corresponding cell value). The dict will *not* contain keys
    for columns where the cell was empty.

    :param url: Url to fetch and parse
    :return: List[Dict]
    """
    if not url:
        return []

    response = requests.get(url)
    response.raise_for_status()

    reader = csv.DictReader(response.text.splitlines())
    # Collect all the rows into a list of dicts while stripping out (valfilter) cells where the
    # value is an empty string (identity returns a Falsy value).
    return [valfilter(identity, row) for row in reader]


@op
def get_df_from_url(url: str) -> pd.DataFrame:
    """Download and return a pandas DataFrame from the URL of a TSV file.

    :param url: raw URL of the TSV file to be downloaded as a DataFrame
    :return: pandas DataFrame of TSV data
    """
    if not url:
        return pd.DataFrame()

    response = requests.get(url)
    response.raise_for_status()

    # Using Pandas read_csv to directly read the file-like object
    df = pd.read_csv(url, delimiter="\t")

    return df


@op
def site_code_mapping() -> dict:
    endpoint = "https://data.neonscience.org/api/v0/sites/"
    response = requests.get(endpoint)
    if response.status_code == 200:
        sites_data = response.json()
        site_code_mapping = {
            site["siteCode"]: f"USA: {site['stateName']}, {site['siteName']}".replace(
                " NEON", ""
            )
            for site in sites_data["data"]
        }
        return site_code_mapping
    else:
        raise Exception(
            f"Failed to fetch site data from {endpoint}. Status code: {response.status_code}, Content: {response.content}"
        )


@op(required_resource_keys={"mongo"})
def materialize_alldocs(context) -> int:
    """
    This function re-creates the alldocs collection to reflect the current state of the Mongo database.
    See nmdc-runtime/docs/nb/bulk_validation_referential_integrity_check.ipynb for more details.
    """
    mdb = context.resources.mongo.db
    schema_view = nmdc_schema_view()

    # batch size for writing documents to alldocs
    BULK_WRITE_BATCH_SIZE = 2000

    # TODO include functional_annotation_agg  for "real-time" ref integrity checking.
    #   For now, production use cases for materialized `alldocs` are limited to `id`-having collections.
    collection_names = populated_schema_collection_names_with_id_field(mdb)
    context.log.info(f"constructing `alldocs` collection using {collection_names=}")

    document_class_names = set(
        chain.from_iterable(collection_name_to_class_names.values())
    )

    cls_slot_map = {
        cls_name: {
            slot.name: slot for slot in schema_view.class_induced_slots(cls_name)
        }
        for cls_name in document_class_names
    }

    # Any ancestor of a document class is a document-referencable range,
    # i.e., a valid range of a document-reference-ranged slot.
    document_referenceable_ranges = set(
        chain.from_iterable(
            schema_view.class_ancestors(cls_name) for cls_name in document_class_names
        )
    )

    document_reference_ranged_slots = defaultdict(list)
    for cls_name, slot_map in cls_slot_map.items():
        for slot_name, slot in slot_map.items():
            if (
                set(get_names_of_classes_in_effective_range_of_slot(schema_view, slot))
                & document_referenceable_ranges
            ):
                document_reference_ranged_slots[cls_name].append(slot_name)

    # Build `alldocs` to a temporary collection for atomic replacement
    # https://www.mongodb.com/docs/v6.0/reference/method/db.collection.renameCollection/#resource-locking-in-replica-sets
    temp_alldocs_collection_name = f"tmp.alldocs.{ObjectId()}"
    temp_alldocs_collection = mdb[temp_alldocs_collection_name]
    context.log.info(f"constructing `{temp_alldocs_collection.name}` collection")

    for coll_name in collection_names:
        context.log.info(f"{coll_name=}")
        write_operations = []
        documents_processed_counter = 0
        for doc in mdb[coll_name].find():
            try:
                doc_type = doc["type"][5:]  # lop off "nmdc:" prefix
            except KeyError:
                raise Exception(
                    f"doc {doc['id']} in collection {coll_name} has no 'type'!"
                )
            slots_to_include = ["id", "type"] + document_reference_ranged_slots[
                doc_type
            ]
            new_doc = keyfilter(lambda slot: slot in slots_to_include, doc)
            new_doc["_type_and_ancestors"] = schema_view.class_ancestors(doc_type)
            write_operations.append(InsertOne(new_doc))
            if len(write_operations) == BULK_WRITE_BATCH_SIZE:
                _ = temp_alldocs_collection.bulk_write(write_operations, ordered=False)
                write_operations.clear()
                documents_processed_counter += BULK_WRITE_BATCH_SIZE
        if len(write_operations) > 0:
            _ = temp_alldocs_collection.bulk_write(write_operations, ordered=False)
            documents_processed_counter += len(write_operations)
        context.log.info(
            f"Inserted {documents_processed_counter} documents from {coll_name=} "
        )

    context.log.info(
        f"produced `{temp_alldocs_collection.name}` collection with"
        f" {temp_alldocs_collection.estimated_document_count()} docs."
    )

    context.log.info(f"creating indexes on `{temp_alldocs_collection.name}` ...")
    # Ensure unique index on "id". Index creation here is blocking (i.e. background=False),
    # so that `temp_alldocs_collection` will be "good to go" on renaming.
    temp_alldocs_collection.create_index("id", unique=True)
    # Add indexes to improve performance of `GET /data_objects/study/{study_id}`:
    slots_to_index = ["has_input", "has_output", "was_informed_by"]
    [temp_alldocs_collection.create_index(slot) for slot in slots_to_index]
    context.log.info(f"created indexes on id, {slots_to_index}.")

    context.log.info(f"renaming `{temp_alldocs_collection.name}` to `alldocs`...")
    temp_alldocs_collection.rename("alldocs", dropTarget=True)

    return mdb.alldocs.estimated_document_count()


@op(config_schema={"nmdc_study_id": str}, required_resource_keys={"mongo"})
def get_ncbi_export_pipeline_study(context: OpExecutionContext) -> Any:
    nmdc_study = find_study_by_id(
        context.op_config["nmdc_study_id"], context.resources.mongo.db
    )
    return nmdc_study


@op(
    config_schema={
        "nmdc_ncbi_attribute_mapping_file_url": str,
        "ncbi_submission_metadata": Field(
            Permissive(
                {
                    "organization": String,
                }
            ),
            is_required=True,
            description="General metadata about the NCBI submission.",
        ),
        "ncbi_biosample_metadata": Field(
            Permissive(
                {
                    "organism_name": String,
                }
            ),
            is_required=True,
            description="Metadata for one or many NCBI BioSample in the Submission.",
        ),
    },
    out=Out(Dict),
)
def get_ncbi_export_pipeline_inputs(context: OpExecutionContext) -> str:
    nmdc_ncbi_attribute_mapping_file_url = context.op_config[
        "nmdc_ncbi_attribute_mapping_file_url"
    ]
    ncbi_submission_metadata = context.op_config.get("ncbi_submission_metadata", {})
    ncbi_biosample_metadata = context.op_config.get("ncbi_biosample_metadata", {})

    return {
        "nmdc_ncbi_attribute_mapping_file_url": nmdc_ncbi_attribute_mapping_file_url,
        "ncbi_submission_metadata": ncbi_submission_metadata,
        "ncbi_biosample_metadata": ncbi_biosample_metadata,
    }


@op(required_resource_keys={"mongo"})
def get_data_objects_from_biosamples(context: OpExecutionContext, biosamples: list):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    biosample_data_objects = fetch_data_objects_from_biosamples(
        alldocs_collection, biosamples
    )
    return biosample_data_objects


@op(required_resource_keys={"mongo"})
def get_nucleotide_sequencing_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    biosample_omics_processing = fetch_nucleotide_sequencing_from_biosamples(
        alldocs_collection, biosamples
    )
    return biosample_omics_processing


@op(required_resource_keys={"mongo"})
def get_library_preparation_from_biosamples(
    context: OpExecutionContext, biosamples: list
):
    mdb = context.resources.mongo.db
    alldocs_collection = mdb["alldocs"]
    biosample_lib_prep = fetch_library_preparation_from_biosamples(
        alldocs_collection, biosamples
    )
    return biosample_lib_prep


@op(required_resource_keys={"mongo"})
def get_all_instruments(context: OpExecutionContext):
    mdb = context.resources.mongo.db
    instrument_set_collection = mdb["instrument_set"]
    all_instruments = get_instruments(instrument_set_collection)
    return all_instruments


@op
def ncbi_submission_xml_from_nmdc_study(
    context: OpExecutionContext,
    nmdc_study: Any,
    ncbi_exporter_metadata: dict,
    biosamples: list,
    omics_processing_records: list,
    data_object_records: list,
    library_preparation_records: list,
    all_instruments: dict,
) -> str:
    ncbi_exporter = NCBISubmissionXML(nmdc_study, ncbi_exporter_metadata)
    ncbi_xml = ncbi_exporter.get_submission_xml(
        biosamples,
        omics_processing_records,
        data_object_records,
        library_preparation_records,
        all_instruments,
    )
    return ncbi_xml


@op
def post_submission_portal_biosample_ingest_record_stitching_filename(
    nmdc_study_id: str,
) -> str:
    filename = nmdc_study_id_to_filename(nmdc_study_id)
    return f"missing_database_records_for_{filename}.json"


@op(
    config_schema={
        "nmdc_study_id": str,
        "gold_nmdc_instrument_mapping_file_url": str,
    },
    out={
        "nmdc_study_id": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
    },
)
def get_database_updater_inputs(context: OpExecutionContext) -> Tuple[str, str]:
    return (
        context.op_config["nmdc_study_id"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
    )


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_data_generation_set_post_biosample_ingest(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
    )
    database = (
        database_updater.generate_data_generation_set_records_from_gold_api_for_study()
    )

    return database


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_biosample_set_for_nmdc_study_from_gold(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
    )
    database = database_updater.generate_biosample_set_from_gold_api_for_study()

    return database
