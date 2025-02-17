import json

from typing import Any

from dagster import (
    repository,
    ScheduleDefinition,
    asset_sensor,
    AssetKey,
    SkipReason,
    RunRequest,
    sensor,
    run_status_sensor,
    DagsterRunStatus,
    RunStatusSensorContext,
    DefaultSensorStatus,
)
from starlette import status
from toolz import merge, get_in

from nmdc_runtime.api.core.util import dotted_path_for
from nmdc_runtime.api.models.job import Job
from nmdc_runtime.api.models.operation import ObjectPutMetadata
from nmdc_runtime.api.models.run import _add_run_fail_event
from nmdc_runtime.api.models.trigger import Trigger
from nmdc_runtime.site.export.study_metadata import export_study_biosamples_metadata
from nmdc_runtime.site.graphs import (
    generate_biosample_set_from_samples_in_gold,
    translate_metadata_submission_to_nmdc_schema_database,
    ingest_metadata_submission,
    gold_study_to_database,
    gold_translation,
    gold_translation_curation,
    create_objects_from_site_object_puts,
    housekeeping,
    ensure_jobs,
    apply_changesheet,
    apply_metadata_in,
    hello_graph,
    translate_neon_api_soil_metadata_to_nmdc_schema_database,
    translate_neon_api_benthic_metadata_to_nmdc_schema_database,
    translate_neon_api_surface_water_metadata_to_nmdc_schema_database,
    ingest_neon_soil_metadata,
    ingest_neon_benthic_metadata,
    ingest_neon_surface_water_metadata,
    ensure_alldocs,
    nmdc_study_to_ncbi_submission_export,
    generate_data_generation_set_for_biosamples_in_nmdc_study,
)
from nmdc_runtime.site.resources import (
    get_mongo,
    runtime_api_site_client_resource,
    runtime_api_user_client_resource,
    nmdc_portal_api_client_resource,
    gold_api_client_resource,
    neon_api_client_resource,
    mongo_resource,
)
from nmdc_runtime.site.resources import (
    get_runtime_api_site_client,
)
from nmdc_runtime.site.translation.emsl import emsl_job, test_emsl_job
from nmdc_runtime.site.translation.gold import gold_job, test_gold_job
from nmdc_runtime.site.translation.jgi import jgi_job, test_jgi_job
from nmdc_runtime.util import freeze
from nmdc_runtime.util import unfreeze

resource_defs = {
    "runtime_api_site_client": runtime_api_site_client_resource,
    "runtime_api_user_client": runtime_api_user_client_resource,
    "nmdc_portal_api_client": nmdc_portal_api_client_resource,
    "gold_api_client": gold_api_client_resource,
    "neon_api_client": neon_api_client_resource,
    "mongo": mongo_resource,
}

preset_normal = {
    "config": {
        "resources": {
            "runtime_api_site_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "site_id": {"env": "API_SITE_ID"},
                    "client_id": {"env": "API_SITE_CLIENT_ID"},
                    "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                },
            },
            "runtime_api_user_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "username": {"env": "API_ADMIN_USER"},
                    "password": {"env": "API_ADMIN_PASS"},
                },
            },
            "mongo": {
                "config": {
                    "host": {"env": "MONGO_HOST"},
                    "username": {"env": "MONGO_USERNAME"},
                    "password": {"env": "MONGO_PASSWORD"},
                    "dbname": {"env": "MONGO_DBNAME"},
                },
            },
        },
        "ops": {},
    },
    "resource_defs": resource_defs,
}

run_config_frozen__normal_env = freeze(preset_normal["config"])

housekeeping_weekly = ScheduleDefinition(
    name="housekeeping_weekly",
    cron_schedule="45 6 * * 1",
    execution_timezone="America/New_York",
    job=housekeeping.to_job(**preset_normal),
)

ensure_alldocs_daily = ScheduleDefinition(
    name="daily_ensure_alldocs",
    cron_schedule="0 3 * * *",
    execution_timezone="America/New_York",
    job=ensure_alldocs.to_job(**preset_normal),
)


def asset_materialization_metadata(asset_event, key):
    """Get metadata from an asset materialization event.

    Example:
    > object_id = asset_materialization_metadata(asset_event, "object_id").text
    """
    for (
        e
    ) in (
        asset_event.dagster_event.step_materialization_data.materialization.metadata_entries
    ):
        if e.label == key:
            return e.entry_data
    return None


@sensor(
    job=ensure_jobs.to_job(name="ensure_job_triggered", **preset_normal),
    default_status=DefaultSensorStatus.RUNNING,
)
def process_workflow_job_triggers(_context):
    """Post a workflow job for each new object with an object_type matching an active trigger.
    (Source: nmdc_runtime.api.boot.triggers).
    """
    mdb = get_mongo(run_config_frozen__normal_env).db
    triggers = [Trigger(**d) for d in mdb.triggers.find()]
    base_jobs = []
    for t in triggers:
        wf_id, object_type = t.workflow_id, t.object_type_id
        object_ids_of_type = [
            d["id"] for d in mdb.objects.find({"types": object_type}, ["id"])
        ]

        if len(object_ids_of_type) == 0:
            continue

        object_ids_with_existing_jobs = [
            get_in(["config", "object_id"], d)
            for d in mdb.jobs.find(
                {"workflow.id": wf_id, "config.object_id": {"$in": object_ids_of_type}},
                ["config.object_id"],
            )
        ]
        object_ids_needing_jobs = list(
            set(object_ids_of_type) - set(object_ids_with_existing_jobs)
        )
        base_jobs.extend(
            [
                {
                    "workflow": {"id": wf_id},
                    "config": {"object_id": id_},
                }
                for id_ in object_ids_needing_jobs
            ]
        )

    if len(base_jobs) > 0:
        run_config = merge(
            run_config_frozen__normal_env,
            {"ops": {"construct_jobs": {"config": {"base_jobs": base_jobs}}}},
        )
        run_key = str(
            tuple(
                sorted(
                    (get_in(["workflow", "id"], b), get_in(["config", "object_id"], b))
                    for b in base_jobs
                )
            )
        )
        yield RunRequest(run_key=run_key, run_config=unfreeze(run_config))
    else:
        yield SkipReason("No new jobs required")


@asset_sensor(
    asset_key=AssetKey(["object", "nmdc_database.json.zip"]),
    job=ensure_jobs.to_job(name="ensure_gold_translation", **preset_normal),
)
def ensure_gold_translation_job(_context, asset_event):
    mdb = get_mongo(run_config_frozen__normal_env).db
    gold_etl_latest = mdb.objects.find_one(
        {"name": "nmdc_database.json.zip"}, sort=[("created_time", -1)]
    )
    sensed_object_id = asset_materialization_metadata(asset_event, "object_id").text
    if gold_etl_latest is None:
        yield SkipReason("can't find sensed asset object_id in database")
        return
    elif gold_etl_latest["id"] != sensed_object_id:
        yield SkipReason("later object than sensed materialization")
        return

    run_config = merge(
        run_config_frozen__normal_env,
        {
            "solids": {
                "construct_jobs": {
                    "config": {
                        "base_jobs": [
                            {
                                "workflow": {"id": "gold-translation-1.0.0"},
                                "config": {"object_id": gold_etl_latest["id"]},
                            }
                        ]
                    }
                }
            }
        },
    )
    yield RunRequest(run_key=sensed_object_id, run_config=unfreeze(run_config))


@asset_sensor(
    asset_key=AssetKey(["job", "gold-translation-1.0.0"]),
    job=gold_translation_curation.to_job(**preset_normal),
)
def claim_and_run_gold_translation_curation(_context, asset_event):
    client = get_runtime_api_site_client(run_config_frozen__normal_env)
    mdb = get_mongo(run_config_frozen__normal_env).db
    object_id_latest = asset_materialization_metadata(
        asset_event, "object_id_latest"
    ).text
    job = mdb.jobs.find_one(
        {
            "workflow.id": "gold-translation-1.0.0",
            "config.object_id_latest": object_id_latest,
        }
    )
    if job is not None:
        rv = client.claim_job(job["id"])
        if rv.status_code == status.HTTP_200_OK:
            operation = rv.json()
            run_config = merge(
                run_config_frozen__normal_env,
                {
                    "ops": {
                        "get_operation": {
                            "config": {
                                "operation_id": operation["id"],
                            }
                        }
                    }
                },
            )
            yield RunRequest(run_key=operation["id"], run_config=unfreeze(run_config))
        else:
            yield SkipReason("Job found, but already claimed by this site")
    else:
        yield SkipReason("No job found")


@sensor(
    job=apply_metadata_in.to_job(name="apply_metadata_in_sensed", **preset_normal),
    default_status=DefaultSensorStatus.RUNNING,
)
def claim_and_run_metadata_in_jobs(_context):
    """
    claims job, and updates job operations with results and marking as done
    """
    client = get_runtime_api_site_client(run_config_frozen__normal_env)
    mdb = get_mongo(run_config_frozen__normal_env).db
    jobs = [
        Job(**d)
        for d in mdb.jobs.find(
            {
                "workflow.id": "metadata-in-1.0.0",
                "claims.site_id": {"$ne": client.site_id},
            }
        )
    ]

    if not jobs:
        yield SkipReason("All relevant jobs already claimed by this site")
        return

    yielded_run_request = False
    skip_notes = []

    jobs = jobs[:5]  # at most five at a time
    for job in jobs:
        job_op_for_site = mdb.operations.find_one(
            {"metadata.job.id": job.id, "metadata.site_id": client.site_id}
        )
        if job_op_for_site is not None:
            skip_notes.append(f"Job {job.id} found, but already claimed by this site")
            # Ensure claim listed in job doc
            op_id = job_op_for_site["id"]
            mdb.jobs.update_one(
                {"id": job.id},
                {"$addToSet": {"claims": {"op_id": op_id, "site_id": client.site_id}}},
            )
        else:
            rv = client.claim_job(job.id)
            if rv.status_code == status.HTTP_200_OK:
                operation = rv.json()
                run_config = merge(
                    run_config_frozen__normal_env,
                    {
                        "ops": {
                            "get_json_in": {
                                "config": {
                                    "object_id": job.config.get("object_id"),
                                }
                            },
                            "perform_mongo_updates": {
                                "config": {"operation_id": operation["id"]}
                            },
                        }
                    },
                )
                yield RunRequest(
                    run_key=operation["id"], run_config=unfreeze(run_config)
                )
                yielded_run_request = True
            else:
                skip_notes.append(
                    f"Job {job.id} found and unclaimed by this site, but claim failed."
                )
    else:
        skip_notes.append("No jobs found")

    if not yielded_run_request:
        yield SkipReason("; ".join(skip_notes))


@sensor(
    job=apply_changesheet.to_job(**preset_normal),
    default_status=DefaultSensorStatus.RUNNING,
)
def claim_and_run_apply_changesheet_jobs(_context):
    """
    claims job, and updates job operations with results and marking as done
    """
    client = get_runtime_api_site_client(run_config_frozen__normal_env)
    mdb = get_mongo(run_config_frozen__normal_env).db
    jobs = [Job(**d) for d in mdb.jobs.find({"workflow.id": "apply-changesheet-1.0.0"})]

    if mdb.operations.count_documents(
        {
            "metadata.job.id": {"$in": [job.id for job in jobs]},
            "metadata.site_id": client.site_id,
        }
    ) == len(jobs):
        yield SkipReason("All relevant jobs already claimed by this site")
        return

    yielded_run_request = False
    skip_notes = []

    for job in jobs:
        job_op_for_site = mdb.operations.find_one(
            {"metadata.job.id": job.id, "metadata.site_id": client.site_id}
        )
        if job_op_for_site is not None:
            skip_notes.append(f"Job {job.id} found, but already claimed by this site")
        else:
            rv = client.claim_job(job.id)
            if rv.status_code == status.HTTP_200_OK:
                operation = rv.json()
                run_config = merge(
                    run_config_frozen__normal_env,
                    {
                        "ops": {
                            "get_changesheet_in": {
                                "config": {
                                    "object_id": job.config.get("object_id"),
                                }
                            },
                            "perform_changesheet_updates": {
                                "config": {"operation_id": operation["id"]}
                            },
                        }
                    },
                )
                yield RunRequest(
                    run_key=operation["id"], run_config=unfreeze(run_config)
                )
                yielded_run_request = True
            else:
                skip_notes.append(
                    f"Job {job.id} found and unclaimed by this site, but claim failed."
                )
    else:
        skip_notes.append("No jobs found")

    if not yielded_run_request:
        yield SkipReason("; ".join(skip_notes))


# TODO ensure data_object_type values from file_type_enum
#    see /metadata-translation/notebooks/202106_curation_updates.ipynb
#    for details ("Create file_type_enum collection" section).


@sensor(job=create_objects_from_site_object_puts.to_job(**preset_normal))
def done_object_put_ops(_context):
    client = get_runtime_api_site_client(run_config_frozen__normal_env)
    ops = [
        op.model_dump()
        for op in client.list_operations(
            {
                "filter": json.dumps(
                    {
                        "done": True,
                        "metadata.model": dotted_path_for(ObjectPutMetadata),
                    }
                )
            }
        )
    ]
    should_run = len(ops) > 0
    run_config = merge(run_config_frozen__normal_env, {})  # ensures this is a dict
    if should_run:
        run_key = ",".join(sorted([op["id"] for op in ops]))
        yield RunRequest(run_key=run_key, run_config=unfreeze(run_config))


@run_status_sensor(run_status=DagsterRunStatus.FAILURE)
def on_run_fail(context: RunStatusSensorContext):
    mdb = get_mongo(run_config_frozen__normal_env).db
    dagster_run_id = context.dagster_run.run_id
    run_event_doc = mdb.run_events.find_one(
        {"run.facets.nmdcRuntime_dagsterRunId": dagster_run_id}
    )
    if run_event_doc is not None:
        nmdc_run_id = run_event_doc["run"]["id"]
        _add_run_fail_event(run_id=nmdc_run_id, mdb=mdb)
        return nmdc_run_id


@repository
def repo():
    graph_jobs = [
        gold_translation.to_job(**preset_normal),
        hello_graph.to_job(name="hello_job"),
        ensure_jobs.to_job(**preset_normal),
        apply_metadata_in.to_job(**preset_normal),
        export_study_biosamples_metadata.to_job(**preset_normal),
        ensure_alldocs.to_job(**preset_normal),
    ]
    schedules = [housekeeping_weekly, ensure_alldocs_daily]
    sensors = [
        done_object_put_ops,
        ensure_gold_translation_job,
        claim_and_run_gold_translation_curation,
        process_workflow_job_triggers,
        claim_and_run_apply_changesheet_jobs,
        claim_and_run_metadata_in_jobs,
        on_run_fail,
    ]

    return graph_jobs + schedules + sensors


@repository
def translation():
    graph_jobs = [jgi_job, gold_job, emsl_job]

    return graph_jobs


@repository
def test_translation():
    graph_jobs = [test_jgi_job, test_gold_job, test_emsl_job]

    return graph_jobs


@repository
def biosample_submission_ingest():
    normal_resources = run_config_frozen__normal_env["resources"]
    return [
        gold_study_to_database.to_job(
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "gold_api_client": {
                            "config": {
                                "base_url": {"env": "GOLD_API_BASE_URL"},
                                "username": {"env": "GOLD_API_USERNAME"},
                                "password": {"env": "GOLD_API_PASSWORD"},
                            },
                        },
                    },
                ),
                "ops": {
                    "get_gold_study_pipeline_inputs": {
                        "config": {
                            "study_id": "",
                            "study_type": "research_study",
                            "gold_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/berkeley-schema-fy24/main/assets/misc/gold_seqMethod_to_nmdc_instrument_set.tsv",
                            "include_field_site_info": False,
                        },
                    },
                    "export_json_to_drs": {"config": {"username": ""}},
                },
            },
        ),
        translate_metadata_submission_to_nmdc_schema_database.to_job(
            description="This job fetches a submission portal entry and translates it into an equivalent nmdc:Database object. The object is serialized to JSON and stored in DRS. This can be considered a dry-run for the `ingest_metadata_submission` job.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "nmdc_portal_api_client": {
                            "config": {
                                "base_url": {"env": "NMDC_PORTAL_API_BASE_URL"},
                                "refresh_token": {
                                    "env": "NMDC_PORTAL_API_REFRESH_TOKEN"
                                },
                            }
                        }
                    },
                ),
                "ops": {
                    "export_json_to_drs": {"config": {"username": "..."}},
                    "get_submission_portal_pipeline_inputs": {
                        "inputs": {
                            "submission_id": "",
                            "nucleotide_sequencing_mapping_file_url": None,
                            "data_object_mapping_file_url": None,
                            "biosample_extras_file_url": None,
                            "biosample_extras_slot_mapping_file_url": None,
                        }
                    },
                    "translate_portal_submission_to_nmdc_schema_database": {
                        "inputs": {
                            "study_category": "research_study",
                            "study_doi_category": None,
                            "study_doi_provider": None,
                            "study_pi_image_url": None,
                        }
                    },
                },
            },
        ),
        ingest_metadata_submission.to_job(
            description="This job fetches a submission portal entry and translates it into an equivalent nmdc:Database object. This object is validated and ingested into Mongo via a `POST /metadata/json:submit` request.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "nmdc_portal_api_client": {
                            "config": {
                                "base_url": {"env": "NMDC_PORTAL_API_BASE_URL"},
                                "refresh_token": {
                                    "env": "NMDC_PORTAL_API_REFRESH_TOKEN"
                                },
                            }
                        }
                    },
                ),
                "ops": {
                    "get_submission_portal_pipeline_inputs": {
                        "inputs": {
                            "submission_id": "",
                            "nucleotide_sequencing_mapping_file_url": None,
                            "data_object_mapping_file_url": None,
                            "biosample_extras_file_url": None,
                            "biosample_extras_slot_mapping_file_url": None,
                        }
                    },
                    "translate_portal_submission_to_nmdc_schema_database": {
                        "inputs": {
                            "study_category": None,
                            "study_doi_category": None,
                            "study_doi_provider": None,
                            "study_pi_image_url": None,
                        }
                    },
                },
            },
        ),
        translate_neon_api_soil_metadata_to_nmdc_schema_database.to_job(
            description="This job fetches the metadata associated with a given NEON data product code and translates it into an equivalent nmdc:Database object. The object is serialized to JSON and stored in DRS. This can be considered a dry-run for the `ingest_neon_metadata` job.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        },
                        "mongo": {
                            "config": {
                                "dbname": {"env": "MONGO_DBNAME"},
                                "host": {"env": "MONGO_HOST"},
                                "password": {"env": "MONGO_PASSWORD"},
                                "username": {"env": "MONGO_USERNAME"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                    },
                ),
                "ops": {
                    "export_json_to_drs": {"config": {"username": "..."}},
                    "get_neon_pipeline_mms_data_product": {
                        "config": {
                            "mms_data_product": {
                                "product_id": "DP1.10107.001",
                                "product_tables": "mms_metagenomeDnaExtraction, mms_metagenomeSequencing",
                            }
                        }
                    },
                    "get_neon_pipeline_sls_data_product": {
                        "config": {
                            "sls_data_product": {
                                "product_id": "DP1.10086.001",
                                "product_tables": "sls_metagenomicsPooling, sls_soilCoreCollection, sls_soilChemistry, sls_soilMoisture, sls_soilpH, ntr_externalLab, ntr_internalLab",
                            }
                        }
                    },
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                },
            },
        ),
        ingest_neon_soil_metadata.to_job(
            description="This job fetches the metadata associated with a given data product code and translates it into an equivalent nmdc:Database object. This object is validated and ingested into Mongo via a `POST /metadata/json:submit` request.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        }
                    },
                ),
                "ops": {
                    "get_neon_pipeline_mms_data_product": {
                        "config": {
                            "mms_data_product": {
                                "product_id": "DP1.10107.001",
                                "product_tables": "mms_metagenomeDnaExtraction, mms_metagenomeSequencing",
                            }
                        }
                    },
                    "get_neon_pipeline_sls_data_product": {
                        "config": {
                            "sls_data_product": {
                                "product_id": "DP1.10086.001",
                                "product_tables": "sls_metagenomicsPooling, sls_soilCoreCollection, sls_soilChemistry, sls_soilMoisture, sls_soilpH, ntr_externalLab, ntr_internalLab",
                            }
                        }
                    },
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                },
            },
        ),
        translate_neon_api_benthic_metadata_to_nmdc_schema_database.to_job(
            description="This job fetches the metadata associated with a given NEON data product code and translates it into an equivalent nmdc:Database object. The object is serialized to JSON and stored in DRS. This can be considered a dry-run for the `ingest_neon_metadata` job.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        },
                        "mongo": {
                            "config": {
                                "dbname": {"env": "MONGO_DBNAME"},
                                "host": {"env": "MONGO_HOST"},
                                "password": {"env": "MONGO_PASSWORD"},
                                "username": {"env": "MONGO_USERNAME"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                    },
                ),
                "ops": {
                    "export_json_to_drs": {"config": {"username": "..."}},
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                    "get_neon_pipeline_benthic_data_product": {
                        "config": {
                            "benthic_data_product": {
                                "product_id": "DP1.20279.001",
                                "product_tables": "mms_benthicMetagenomeSequencing, mms_benthicMetagenomeDnaExtraction, mms_benthicRawDataFiles, amb_fieldParent",
                            }
                        }
                    },
                },
            },
        ),
        ingest_neon_benthic_metadata.to_job(
            description="",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        }
                    },
                ),
                "ops": {
                    "get_neon_pipeline_benthic_data_product": {
                        "config": {
                            "benthic_data_product": {
                                "product_id": "DP1.20279.001",
                                "product_tables": "mms_benthicMetagenomeSequencing, mms_benthicMetagenomeDnaExtraction, mms_benthicRawDataFiles, amb_fieldParent",
                            }
                        }
                    },
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                },
            },
        ),
        translate_neon_api_surface_water_metadata_to_nmdc_schema_database.to_job(
            description="This job fetches the metadata associated with a given NEON data product code and translates it into an equivalent nmdc:Database object. The object is serialized to JSON and stored in DRS. This can be considered a dry-run for the `ingest_neon_metadata` job.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        },
                        "mongo": {
                            "config": {
                                "dbname": {"env": "MONGO_DBNAME"},
                                "host": {"env": "MONGO_HOST"},
                                "password": {"env": "MONGO_PASSWORD"},
                                "username": {"env": "MONGO_USERNAME"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                    },
                ),
                "ops": {
                    "export_json_to_drs": {"config": {"username": "..."}},
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                    "get_neon_pipeline_surface_water_data_product": {
                        "config": {
                            "surface_water_data_product": {
                                "product_id": "DP1.20281.001",
                                "product_tables": "mms_swMetagenomeSequencing, mms_swMetagenomeDnaExtraction, amc_fieldGenetic, amc_fieldSuperParent, mms_swRawDataFiles",
                            }
                        }
                    },
                },
            },
        ),
        ingest_neon_surface_water_metadata.to_job(
            description="",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "neon_api_client": {
                            "config": {
                                "base_url": {"env": "NEON_API_BASE_URL"},
                                "api_token": {"env": "NEON_API_TOKEN"},
                            },
                        }
                    },
                ),
                "ops": {
                    "get_neon_pipeline_surface_water_data_product": {
                        "config": {
                            "surface_water_data_product": {
                                "product_id": "DP1.20281.001",
                                "product_tables": "mms_swMetagenomeSequencing, mms_swMetagenomeDnaExtraction, amc_fieldGenetic, amc_fieldSuperParent, mms_swRawDataFiles",
                            }
                        }
                    },
                    "get_neon_pipeline_inputs": {
                        "inputs": {
                            "neon_envo_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv",
                            "neon_raw_data_file_mappings_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/misc/neon_raw_data_file_mappings.tsv",
                            "neon_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/neon_sequencingMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                },
            },
        ),
    ]


@repository
def biosample_export():
    normal_resources = run_config_frozen__normal_env["resources"]
    return [
        nmdc_study_to_ncbi_submission_export.to_job(
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "mongo": {
                            "config": {
                                "host": {"env": "MONGO_HOST"},
                                "username": {"env": "MONGO_USERNAME"},
                                "password": {"env": "MONGO_PASSWORD"},
                                "dbname": {"env": "MONGO_DBNAME"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                    },
                ),
                "ops": {
                    "get_ncbi_export_pipeline_study": {
                        "config": {
                            "nmdc_study_id": "",
                        }
                    },
                    "get_ncbi_export_pipeline_inputs": {
                        "config": {
                            "nmdc_ncbi_attribute_mapping_file_url": "",
                            "ncbi_submission_metadata": {
                                "organization": "",
                            },
                            "ncbi_biosample_metadata": {
                                "organism_name": "",
                            },
                        }
                    },
                },
            },
        ),
    ]


@repository
def database_records_stitching():
    normal_resources = run_config_frozen__normal_env["resources"]
    return [
        generate_data_generation_set_for_biosamples_in_nmdc_study.to_job(
            description="This job can be used to create a data_generation_set JSON for biosamples that are already present in the NMDC database.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "runtime_api_user_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "username": {"env": "API_ADMIN_USER"},
                                "password": {"env": "API_ADMIN_PASS"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                        "gold_api_client": {
                            "config": {
                                "base_url": {"env": "GOLD_API_BASE_URL"},
                                "username": {"env": "GOLD_API_USERNAME"},
                                "password": {"env": "GOLD_API_PASSWORD"},
                            },
                        },
                    },
                ),
                "ops": {
                    "get_database_updater_inputs": {
                        "config": {
                            "nmdc_study_id": "",
                            "gold_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/gold_seqMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                    "export_json_to_drs": {"config": {"username": ""}},
                },
            },
        ),
        generate_biosample_set_from_samples_in_gold.to_job(
            description="This job can be used to create a biosample_set JSON from samples in GOLD for a given study in NMDC.",
            resource_defs=resource_defs,
            config={
                "resources": merge(
                    unfreeze(normal_resources),
                    {
                        "runtime_api_user_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "username": {"env": "API_ADMIN_USER"},
                                "password": {"env": "API_ADMIN_PASS"},
                            },
                        },
                        "runtime_api_site_client": {
                            "config": {
                                "base_url": {"env": "API_HOST"},
                                "client_id": {"env": "API_SITE_CLIENT_ID"},
                                "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                                "site_id": {"env": "API_SITE_ID"},
                            },
                        },
                        "gold_api_client": {
                            "config": {
                                "base_url": {"env": "GOLD_API_BASE_URL"},
                                "username": {"env": "GOLD_API_USERNAME"},
                                "password": {"env": "GOLD_API_PASSWORD"},
                            },
                        },
                    },
                ),
                "ops": {
                    "get_database_updater_inputs": {
                        "config": {
                            "nmdc_study_id": "",
                            "gold_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/gold_seqMethod_to_nmdc_instrument_set.tsv",
                        }
                    },
                    "export_json_to_drs": {"config": {"username": ""}},
                },
            },
        ),
    ]


# @repository
# def validation():
#     graph_jobs = [validate_jgi_job, validate_gold_job, validate_emsl_job]
#     return graph_jobs
#
#
# @repository
# def test_validation():
#     graph_jobs = [test_validate_jgi_job, test_validate_gold_job, test_validate_emsl_job]
#     return graph_jobs
