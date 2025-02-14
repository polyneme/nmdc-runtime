import json
import os
import re

import pytest
import requests
from dagster import build_op_context
from starlette import status
from tenacity import wait_random_exponential, stop_after_attempt, retry
from toolz import get_in

from nmdc_runtime.api.core.auth import get_password_hash
from nmdc_runtime.api.core.metadata import df_from_sheet_in, _validate_changesheet
from nmdc_runtime.api.core.util import generate_secret, dotted_path_for
from nmdc_runtime.api.db.mongo import (
    get_mongo_db,
    get_collection_names_from_schema,
    mongorestore_collection,
)
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.job import Job, JobOperationMetadata
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.api.models.site import SiteInDB, SiteClientInDB
from nmdc_runtime.api.models.user import UserInDB, UserIn, User
from nmdc_runtime.site.ops import materialize_alldocs
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import (
    get_mongo,
    RuntimeApiSiteClient,
    mongo_resource,
    RuntimeApiUserClient,
)
from nmdc_runtime.util import REPO_ROOT_DIR, ensure_unique_id_indexes, validate_json
from tests.test_util import download_to


MONGODUMP_URL_PREFIX = os.getenv("MONGO_REMOTE_DUMP_URL_PREFIX")
# extract the specific dump directory name, e.g. 'dump_nmdc-prod_2025-02-12_20-12-02'.
MONGODUMP_DIRNAME = MONGODUMP_URL_PREFIX.rsplit("/", maxsplit=1)[-1]
MONGODUMP_DIR = REPO_ROOT_DIR / "tests" / "nmdcdb" / MONGODUMP_DIRNAME
MONGODUMP_DIR.mkdir(parents=True, exist_ok=True)

# certain schema collections are not expected for tests
SCHEMA_COLLECTIONS_EXCLUDED = {
    "functional_annotation_agg",
    "functional_annotation_set",
    "genome_feature_set",
}


def ensure_schema_collections_local_filesystem_cache():
    # download collections into "nmdc" db namespace
    db_dir = MONGODUMP_DIR / "nmdc"
    db_dir.mkdir(exist_ok=True)
    for name in get_collection_names_from_schema():
        if name in SCHEMA_COLLECTIONS_EXCLUDED:
            continue
        url = f"{MONGODUMP_URL_PREFIX}/nmdc/{name}.bson.gz"
        target_path = db_dir / (name + ".bson.gz")
        # TODO use sha256 hashes or at least file sizes to ensure fidelity of existing files.
        #   Can use HEAD request on `url`?
        if not db_dir.joinpath(name + ".bson.gz").exists():
            download_to(url, target_path)


def ensure_schema_collections_loaded():
    ensure_schema_collections_local_filesystem_cache()
    mdb = get_mongo_db()
    for name in get_collection_names_from_schema():
        if name in SCHEMA_COLLECTIONS_EXCLUDED:
            continue
        if not mdb.get_collection(name).estimated_document_count() > 0:
            filepath = MONGODUMP_DIR / "nmdc" / (name + ".bson.gz")
            print(f"ensure_schema_collections_loaded: restoring {name}")
            mongorestore_collection(mdb, name, filepath)


def ensure_schema_collections_and_alldocs(force_refresh_of_alldocs: bool = False):
    r"""
    This function can be used to ensure properties of schema-described collections and the "alldocs" collection.

    :param bool force_refresh_of_alldocs: Whether you want to force a refresh of the "alldocs" collection,
                                          regardless of whether it is empty or not. By default, this function
                                          will only refresh the "alldocs" collection if it is empty.
    """
    mdb = get_mongo_db()
    ensure_schema_collections_loaded()
    ensure_unique_id_indexes(mdb)
    # Return if `alldocs` collection has already been materialized, and caller does not want to force a refresh of it.
    if mdb.alldocs.estimated_document_count() > 0 and not force_refresh_of_alldocs:
        print(
            "ensure_schema_collections_and_alldocs: `alldocs` collection already materialized"
        )
        return

    print("materializing alldocs...")
    materialize_alldocs(
        build_op_context(
            resources={
                "mongo": mongo_resource.configured(
                    {
                        "dbname": os.getenv("MONGO_DBNAME"),
                        "host": os.getenv("MONGO_HOST"),
                        "password": os.getenv("MONGO_PASSWORD"),
                        "username": os.getenv("MONGO_USERNAME"),
                    }
                )
            }
        )
    )


def ensure_test_resources(mdb):
    username = "testuser"
    password = generate_secret()
    site_id = "testsite"
    mdb.users.replace_one(
        {"username": username},
        UserInDB(
            username=username,
            hashed_password=get_password_hash(password),
            site_admin=[site_id],
        ).model_dump(exclude_unset=True),
        upsert=True,
    )

    client_id = "testsite-testclient"
    client_secret = generate_secret()
    mdb.sites.replace_one(
        {"id": site_id},
        SiteInDB(
            id=site_id,
            clients=[
                SiteClientInDB(
                    id=client_id,
                    hashed_secret=get_password_hash(client_secret),
                )
            ],
        ).model_dump(),
        upsert=True,
    )
    wf_id = "test"
    job_id = "nmdc:fk0jb83"
    prev_ops = {"metadata.job.id": job_id, "metadata.site_id": site_id}
    mdb.operations.delete_many(prev_ops)
    job = Job(**{"id": job_id, "workflow": {"id": wf_id}, "config": {}, "claims": []})
    mdb.jobs.replace_one(
        {"id": job_id}, job.model_dump(exclude_unset=True), upsert=True
    )
    mdb["minter.requesters"].replace_one({"id": site_id}, {"id": site_id}, upsert=True)
    ensure_schema_collections_and_alldocs()
    return {
        "site_client": {
            "site_id": site_id,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        "user": {"username": username, "password": password},
        "job": job.model_dump(exclude_unset=True),
    }


@pytest.fixture
def api_site_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


@pytest.fixture
def api_user_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiUserClient(base_url=os.getenv("API_HOST"), **rs["user"])


@pytest.mark.skip(reason="Skipping because test causes suite to hang")
def test_update_operation():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    client = RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])
    rv = client.claim_job(rs["job"]["id"])
    assert "id" in rv.json()
    op = rv.json()
    new_op = client.update_operation(op["id"], {"metadata": {"foo": "bar"}}).json()
    assert get_in(["metadata", "site_id"], new_op) == rs["site_client"]["site_id"]
    assert get_in(["metadata", "job", "id"], new_op) == rs["job"]["id"]
    assert get_in(["metadata", "model"], new_op) == dotted_path_for(
        JobOperationMetadata
    )


def test_create_user():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    @retry(
        wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(3)
    )
    def get_token():
        """

        Randomly wait up to 2^x * 1 seconds between each retry until the range reaches 60
        seconds, then randomly up to 60 seconds afterwards

        """

        _rv = requests.post(
            base_url + "/token",
            data={
                "grant_type": "password",
                "username": rs["user"]["username"],
                "password": rs["user"]["password"],
            },
        )
        token_response = _rv.json()
        return token_response["access_token"]

    headers = {"Authorization": f"Bearer {get_token()}"}

    user_in = UserIn(username="foo", password=generate_secret())
    mdb.users.delete_one({"username": user_in.username})
    mdb.users.update_one(
        {"username": rs["user"]["username"]},
        {"$addToSet": {"site_admin": "nmdc-runtime-useradmin"}},
    )
    rv = requests.request(
        "POST",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in.model_dump(exclude_unset=True),
    )

    try:
        assert rv.status_code == status.HTTP_201_CREATED
        User(**rv.json())
    finally:
        mdb.users.delete_one({"username": user_in.username})
        mdb.users.update_one(
            {"username": rs["user"]["username"]},
            {"$pull": {"site_admin": "nmdc-runtime-useradmin"}},
        )


def test_update_user():
    mdb = get_mongo(run_config_frozen__normal_env).db
    rs = ensure_test_resources(mdb)
    base_url = os.getenv("API_HOST")

    # Try up to three times, waiting for up to 60 seconds between each attempt.
    @retry(
        wait=wait_random_exponential(multiplier=1, max=60), stop=stop_after_attempt(3)
    )
    def get_token():
        """
        Fetch an auth token from the Runtime API.
        """

        _rv = requests.post(
            base_url + "/token",
            data={
                "grant_type": "password",
                "username": rs["user"]["username"],
                "password": rs["user"]["password"],
            },
        )
        token_response = _rv.json()
        return token_response["access_token"]

    headers = {"Authorization": f"Bearer {get_token()}"}

    user_in1 = UserIn(username="foo", password="oldpass")
    mdb.users.delete_one({"username": user_in1.username})
    mdb.users.update_one(
        {"username": rs["user"]["username"]},
        {"$addToSet": {"site_admin": "nmdc-runtime-useradmin"}},
    )
    rv_create = requests.request(
        "POST",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in1.model_dump(exclude_unset=True),
    )

    u1 = mdb.users.find_one({"username": user_in1.username})

    user_in2 = UserIn(username="foo", password="newpass")

    rv_update = requests.request(
        "PUT",
        url=(base_url + "/users"),
        headers=headers,
        json=user_in2.model_dump(exclude_unset=True),
    )

    u2 = mdb.users.find_one({"username": user_in2.username})
    try:
        assert rv_create.status_code == status.HTTP_201_CREATED
        assert rv_update.status_code == status.HTTP_200_OK
        assert u1["hashed_password"] != u2["hashed_password"]

    finally:
        mdb.users.delete_one({"username": user_in1.username})
        mdb.users.update_one(
            {"username": rs["user"]["username"]},
            {"$pull": {"site_admin": "nmdc-runtime-useradmin"}},
        )


@pytest.fixture
def api_site_client():
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    return RuntimeApiSiteClient(base_url=os.getenv("API_HOST"), **rs["site_client"])


def test_metadata_validate_json_0(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {
            "field_research_site_set": [
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
                {"id": "nmdc:frsite-11-s2dqk408", "name": "BESC-470-CL2_38_23"},
            ]
        },
    )
    assert rv.json()["result"] == "errors"


def test_metadata_validate_json_empty_collection(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": []},
    )
    assert rv.json()["result"] != "errors"


def test_metadata_validate_json_with_type_attribute(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": [], "@type": "Database"},
    )
    assert rv.json()["result"] != "errors"
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"study_set": [], "@type": "nmdc:Database"},
    )
    assert rv.json()["result"] != "errors"


def test_metadata_validate_json_with_unknown_collection(api_site_client):
    rv = api_site_client.request(
        "POST",
        "/metadata/json:validate",
        {"studi_set": []},
    )
    assert rv.json()["result"] == "errors"


def test_submit_changesheet():
    sheet_in = ChangesheetIn(
        name="sheet",
        content_type="text/tab-separated-values",
        text="id\taction\tattribute\tvalue\nnmdc:bsm-12-7mysck21\tupdate\tassociated_studies\tnmdc:sty-11-pzmd0x14\n",
    )
    mdb = get_mongo_db()
    rs = ensure_test_resources(mdb)
    if not mdb.biosample_set.find_one({"id": "nmdc:bsm-12-7mysck21"}):
        mdb.biosample_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-12-7mysck21.json"
                ).read_text()
            )
        )
    if not mdb.study_set.find_one({"id": "nmdc:sty-11-pzmd0x14"}):
        mdb.study_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_sty-11-pzmd0x14.json"
                ).read_text()
            )
        )
    df_change = df_from_sheet_in(sheet_in, mdb)
    _ = _validate_changesheet(df_change, mdb)

    # clear objects collection to avoid strange duplicate key error.
    mdb.objects.delete_many({})

    drs_obj_doc = persist_content_and_get_drs_object(
        content=sheet_in.text,
        username=rs["user"]["username"],
        filename=re.sub(r"[^A-Za-z0-9._\-]", "_", sheet_in.name),
        content_type=sheet_in.content_type,
        description="changesheet",
        id_ns="changesheets",
    )
    mdb.objects.delete_one({"id": drs_obj_doc["id"]})
    assert True


def test_submit_workflow_activities(api_site_client):
    test_collection, test_id = (
        "workflow_execution_set",
        "nmdc:wfrqc-11-t0tvnp52.2",
    )
    test_payload = {
        test_collection: [
            {
                "id": test_id,
                "name": "Read QC Activity for nmdc:wfrqc-11-t0tvnp52.1",
                "started_at_time": "2024-01-11T20:48:30.718133+00:00",
                "ended_at_time": "2024-01-11T21:11:44.884260+00:00",
                "was_informed_by": "nmdc:omprc-11-9mvz7z22",
                "execution_resource": "NERSC-Perlmutter",
                "git_url": "https://github.com/microbiomedata/ReadsQC",
                "has_input": ["nmdc:dobj-11-gpthnj64"],
                "has_output": [
                    "nmdc:dobj-11-w5dak635",
                    "nmdc:dobj-11-g6d71n77",
                    "nmdc:dobj-11-bds7qq03",
                ],
                "type": "nmdc:ReadQcAnalysis",
                "version": "v1.0.8",
            }
        ]
    }
    mdb = get_mongo_db()
    if doc_to_restore := mdb[test_collection].find_one({"id": test_id}):
        mdb[test_collection].delete_one({"id": test_id})
    rv = api_site_client.request(
        "POST",
        "/workflows/workflow_executions",
        test_payload,
    )
    assert rv.json() == {"message": "jobs accepted"}
    rv = api_site_client.request("GET", f"/nmdcschema/ids/{test_id}")
    mdb[test_collection].delete_one({"id": test_id})
    if doc_to_restore:
        mdb[test_collection].insert_one(doc_to_restore)
    assert "id" in rv.json() and "input_read_count" not in rv.json()


def test_get_class_name_and_collection_names_by_doc_id():
    base_url = os.getenv("API_HOST")

    # Seed the database.
    mdb = get_mongo_db()
    study_set_collection = mdb.get_collection(name="study_set")
    my_study = {"id": "nmdc:sty-1-foobar", "type": "nmdc:Study"}
    study_set_collection.replace_one(my_study, my_study, upsert=True)

    # Valid `id`, and the document exists in database.
    id_ = my_study["id"]
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    body = response.json()
    assert response.status_code == 200
    assert body["id"] == id_
    assert body["collection_name"] == "study_set"

    # Valid `id`, but the document does not exist in database.
    id_ = "nmdc:sty-1-bazqux"
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    assert response.status_code == 404

    # Invalid `id` (because "foo" is an invalid typecode).
    id_ = "nmdc:foo-1-foobar"
    response = requests.request(
        "GET", f"{base_url}/nmdcschema/ids/{id_}/collection-name"
    )
    assert response.status_code == 404


def test_find_data_objects_for_nonexistent_study(api_site_client):
    r"""
    Confirms the endpoint returns an unsuccessful status code when no `Study` having the specified `id` exists.
    Reference: https://docs.pytest.org/en/stable/reference/reference.html#pytest.raises

    Note: The `api_site_client` fixture's `request` method will raise an exception if the server responds with
          an unsuccessful status code.
    """
    ensure_schema_collections_and_alldocs()
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            "/data_objects/study/nmdc:sty-11-fake",
        )


def test_find_data_objects_for_study_having_none(api_site_client):
    # Seed the test database with a study having no associated data objects.
    mdb = get_mongo_db()
    study_id = "nmdc:sty-00-beeeeeef"
    study_dict = {
        "id": study_id,
        "type": "nmdc:Study",
        "study_category": "research_study",
    }
    assert validate_json({"study_set": [study_dict]}, mdb)["result"] != "errors"

    mdb.get_collection(name="study_set").replace_one(
        {"id": study_id}, study_dict, upsert=True
    )

    # Update the `alldocs` collection, which is a cache used by the endpoint under test.
    ensure_schema_collections_and_alldocs(force_refresh_of_alldocs=True)

    # Confirm the endpoint responds with no data objects.
    response = api_site_client.request("GET", f"/data_objects/study/{study_id}")
    assert response.status_code == 200
    data_objects_by_biosample = response.json()
    assert len(data_objects_by_biosample) == 0

    # Clean up: Delete the documents we created within this test, from the database.
    mdb.get_collection(name="study_set").delete_one({"id": study_id})
    mdb.get_collection(name="alldocs").delete_many({})


def test_find_data_objects_for_study_having_one(api_site_client):
    # Seed the test database with a study having one associated data object.
    mdb = get_mongo_db()
    study_id = "nmdc:sty-11-r2h77870"
    study_dict = {
        "id": study_id,
        "type": "nmdc:Study",
        "study_category": "research_study",
    }
    fakes = set()
    assert validate_json({"study_set": [study_dict]}, mdb)["result"] != "errors"
    if mdb.get_collection(name="study_set").find_one({"id": study_id}) is None:
        mdb.get_collection(name="study_set").insert_one(study_dict)
        fakes.add("study")
    biosample_id = "nmdc:bsm-11-6zd5nb38"
    biosample_dict = {
        "id": biosample_id,
        "env_broad_scale": {
            "has_raw_value": "ENVO_00000446",
            "term": {
                "id": "ENVO:00000446",
                "name": "terrestrial biome",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "env_local_scale": {
            "has_raw_value": "ENVO_00005801",
            "term": {
                "id": "ENVO:00005801",
                "name": "rhizosphere",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "env_medium": {
            "has_raw_value": "ENVO_00001998",
            "term": {
                "id": "ENVO:00001998",
                "name": "soil",
                "type": "nmdc:OntologyClass",
            },
            "type": "nmdc:ControlledIdentifiedTermValue",
        },
        "type": "nmdc:Biosample",
        "associated_studies": [study_id],
    }
    assert validate_json({"biosample_set": [biosample_dict]}, mdb)["result"] != "errors"
    if mdb.get_collection(name="biosample_set").find_one({"id": biosample_id}) is None:
        mdb.get_collection(name="biosample_set").insert_one(biosample_dict)
        fakes.add("biosample")

    data_generation_id = "nmdc:omprc-11-nmtj1g51"
    data_generation_dict = {
        "id": data_generation_id,
        "has_input": [biosample_id],
        "type": "nmdc:NucleotideSequencing",
        "analyte_category": "metagenome",
        "associated_studies": [study_id],
    }
    assert (
        validate_json({"data_generation_set": [data_generation_dict]}, mdb)["result"]
        != "errors"
    )
    if (
        mdb.get_collection(name="data_generation_set").find_one(
            {"id": data_generation_id}
        )
        is None
    ):
        mdb.get_collection(name="data_generation_set").insert_one(data_generation_dict)
        fakes.add("data_generation")

    data_object_id = "nmdc:dobj-11-cpv4y420"
    data_object_dict = {
        "id": data_object_id,
        "name": "Raw sequencer read data",
        "description": "Metagenome Raw Reads for nmdc:omprc-11-nmtj1g51",
        "type": "nmdc:DataObject",
    }
    assert (
        validate_json({"data_object_set": [data_object_dict]}, mdb)["result"]
        != "errors"
    )
    if (
        mdb.get_collection(name="data_object_set").find_one({"id": data_object_id})
        is None
    ):
        mdb.get_collection(name="data_object_set").insert_one(data_object_dict)
        fakes.add("data_object")

    workflow_execution_id = "nmdc:wfmsa-11-fqq66x60.1"
    workflow_execution_dict = {
        "id": workflow_execution_id,
        "started_at_time": "2023-03-24T02:02:59.479107+00:00",
        "ended_at_time": "2023-03-24T02:02:59.479129+00:00",
        "was_informed_by": data_generation_id,
        "execution_resource": "JGI",
        "git_url": "https://github.com/microbiomedata/RawSequencingData",
        "has_input": [biosample_id],
        "has_output": [data_object_id],
        "type": "nmdc:MetagenomeSequencing",
    }
    assert (
        validate_json({"workflow_execution_set": [workflow_execution_dict]}, mdb)[
            "result"
        ]
        != "errors"
    )
    if (
        mdb.get_collection(name="workflow_execution_set").find_one(
            {"id": workflow_execution_id}
        )
        is None
    ):
        mdb.get_collection(name="workflow_execution_set").insert_one(
            workflow_execution_dict
        )
        fakes.add("workflow_execution")

    # Update the `alldocs` collection, which is a cache used by the endpoint under test.
    ensure_schema_collections_and_alldocs(force_refresh_of_alldocs=True)

    # Confirm the endpoint responds with the data object we inserted above.
    response = api_site_client.request("GET", f"/data_objects/study/{study_id}")
    assert response.status_code == 200
    data_objects_by_biosample = response.json()
    assert any(
        biosample_data_objects["biosample_id"] == biosample_id
        and any(
            do["id"] == data_object_id for do in biosample_data_objects["data_objects"]
        )
        for biosample_data_objects in data_objects_by_biosample
    )

    # Clean up: Delete the documents we created within this test, from the database.
    if "study" in fakes:
        mdb.get_collection(name="study_set").delete_one({"id": study_id})
    if "biosample" in fakes:
        mdb.get_collection(name="biosample_set").delete_one({"id": biosample_id})
    if "data_generation":
        mdb.get_collection(name="data_generation_set").delete_one(
            {"id": data_generation_id}
        )
    if "data_object" in fakes:
        mdb.get_collection(name="data_object_set").delete_one({"id": data_object_id})
    if "workflow_execution" in fakes:
        mdb.get_collection(name="workflow_execution_set").delete_one(
            {"id": workflow_execution_id}
        )

    mdb.get_collection(name="alldocs").delete_many({})


def test_find_planned_processes(api_site_client):
    mdb = get_mongo_db()
    database_dict = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "planned_processes.json").read_text()
    )
    for collection_name, docs in database_dict.items():
        for doc in docs:
            mdb[collection_name].replace_one({"id": doc["id"]}, doc, upsert=True)

    rv = api_site_client.request(
        "GET",
        "/planned_processes",
    )
    assert rv.json()["meta"]["count"] >= 9


def test_find_planned_process_by_id(api_site_client):
    # Seed the database with documents that represent instances of the `PlannedProcess` class or any of its subclasses.
    mdb = get_mongo_db()
    database_dict = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "planned_processes.json").read_text()
    )
    for collection_name, docs in database_dict.items():
        for doc in docs:
            mdb[collection_name].replace_one({"id": doc["id"]}, doc, upsert=True)

    # Also, include a document that represents a `Study` (which is not a subclass of `PlannedProcess`),
    # so we can check whether the endpoint-under-test only searches collections that we expect it to.
    my_study = {"id": "nmdc:sty-1-foobar", "type": "nmdc:Study"}
    mdb.get_collection(name="study_set").replace_one(my_study, my_study, upsert=True)

    # Test case: The `id` belongs to a document that represents an instance of
    #            the `PlannedProcess` class or one of its subclasses.
    rv = api_site_client.request(
        "GET",
        f"/planned_processes/nmdc:wfmag-11-00jn7876.1",
    )
    planned_process = rv.json()
    assert "_id" not in planned_process
    assert planned_process["id"] == "nmdc:wfmag-11-00jn7876.1"

    # Test case: The `id` does not belong to a document.
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            f"/planned_processes/nmdc:wfmag-11-00jn7876.99",
        )

    # Test case: The `id` belongs to a document, but that document does not represent
    #            an instance of the `PlannedProcess` class or any of its subclasses.
    with pytest.raises(requests.exceptions.HTTPError):
        api_site_client.request(
            "GET",
            f"/planned_processes/nmdc:sty-11-00000001",
        )


def _test_run_query_find_as(client):
    mdb = get_mongo_db()
    if not mdb.biosample_set.find_one({"id": "nmdc:bsm-12-7mysck21"}):
        mdb.biosample_set.insert_one(
            json.loads(
                (
                    REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-12-7mysck21.json"
                ).read_text()
            )
        )

    # Make sure user client works
    response = client.request(
        "POST",
        "/queries:run",
        {"find": "biosample_set", "filter": {"id": "nmdc:bsm-12-7mysck21"}},
    )
    assert response.status_code == 200
    assert "cursor" in response.json()


def test_run_query_find_as_user(api_user_client):
    _test_run_query_find_as(api_user_client)


def test_run_query_find_as_site(api_site_client):
    _test_run_query_find_as(api_site_client)


def _test_run_query_delete_as(client):
    client_id_attribute = (
        "username" if isinstance(client, RuntimeApiUserClient) else "client_id"
    )

    mdb = get_mongo_db()
    biosample_id = "nmdc:bsm-12-deleteme"

    if not mdb.biosample_set.find_one({"id": biosample_id}):
        mdb.biosample_set.insert_one({"id": biosample_id})

    # Access should not work without permissions
    mdb["_runtime"].api.allow.delete_many(
        {
            "username": getattr(client, client_id_attribute),
            "action": "/queries:run(query_cmd:DeleteCommand)",
        }
    )
    with pytest.raises(requests.exceptions.HTTPError) as excinfo:
        response = client.request(
            "POST",
            "/queries:run",
            {
                "delete": "biosample_set",
                "deletes": [{"q": {"id": biosample_id}, "limit": 1}],
            },
        )
    assert excinfo.value.response.status_code == 403

    # Add persmissions to DB
    mdb["_runtime"].api.allow.insert_one(
        {
            "username": getattr(client, client_id_attribute),
            "action": "/queries:run(query_cmd:DeleteCommand)",
        }
    )
    try:
        response = client.request(
            "POST",
            "/queries:run",
            {
                "delete": "biosample_set",
                "deletes": [{"q": {"id": biosample_id}, "limit": 1}],
            },
        )
        assert response.status_code == 200
        assert response.json()["n"] == 1
    finally:
        mdb["_runtime"].api.allow.delete_one(
            {"username": getattr(client, client_id_attribute)}
        )


def test_run_query_delete_as_user(api_user_client):
    _test_run_query_delete_as(api_user_client)


def test_run_query_delete_as_site(api_site_client):
    _test_run_query_delete_as(api_site_client)


def test_run_query_update_as_user(api_user_client):
    """Submit a request to store data that does not comply with the schema."""
    mdb = get_mongo_db()
    allow_spec = {
        "username": api_user_client.username,
        "action": "/queries:run(query_cmd:DeleteCommand)",
    }
    mdb["_runtime.api.allow"].replace_one(allow_spec, allow_spec, upsert=True)
    with pytest.raises(requests.HTTPError):
        api_user_client.request(
            "POST",
            "/queries:run",
            {
                "update": "biosample_set",
                "updates": [
                    {
                        "q": {"id": "nmdc:bsm-13-amrnys72"},
                        "u": {"$set": {"associated_studies.0": 42}},
                    }
                ],
            },
        )


def test_run_query_find_with_continuation(api_user_client):
    # Example taken from <https://github.com/microbiomedata/nmdc-runtime/issues/434#issue-2074885218>
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "find": "data_generation_set",
            "filter": {"associated_studies": "nmdc:sty-11-aygzgv51"},
        },
    )
    assert response.status_code == 200
    assert "cursor" in response.json()
    cursor = response.json()["cursor"]
    assert cursor["id"] != "0"  # i.e., there is another batch to fetch
    assert False


def test_run_query_aggregate_with_continuation(api_user_client):
    # Example taken from <https://github.com/microbiomedata/nmdc-runtime/issues/434#issuecomment-2463238444>
    response = api_user_client.request(
        "POST",
        "/queries:run",
        {
            "aggregate": "workflow_execution_set",
            "pipeline": [
                {"$match": {"type": "nmdc:MetagenomeAnnotation", "version": "v1.1.0"}},
                {
                    "$lookup": {
                        "from": "data_generation_set",
                        "localField": "was_informed_by",
                        "foreignField": "id",
                        "as": "data_generation_set",
                    }
                },
                {
                    "$match": {
                        "data_generation_set.associated_studies": "nmdc:sty-11-34xj1150",
                        "data_generation_set.processing_institution": "JGI",
                    }
                },
                {
                    "$lookup": {
                        "from": "data_object_set",
                        "localField": "has_input",
                        "foreignField": "id",
                        "as": "do_input",
                    }
                },
                {
                    "$lookup": {
                        "from": "data_object_set",
                        "localField": "has_output",
                        "foreignField": "id",
                        "as": "do_output",
                    }
                },
            ],
            "allowDiskUse": True,
        },
    )
    assert response.status_code == 200
    assert "cursor" in response.json()
    cursor = response.json()["cursor"]
    assert cursor["id"] != "0"  # i.e., there is another batch to fetch
