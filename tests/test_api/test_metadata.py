import json
from functools import lru_cache
from typing import Optional

import fastjsonschema
import pandas as pd
import pytest

from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.endpoints.util import persist_content_and_get_drs_object
from nmdc_runtime.api.models.metadata import ChangesheetIn
from nmdc_runtime.util import get_nmdc_jsonschema_dict
from toolz import dissoc

from nmdc_runtime.api.core.metadata import (
    load_changesheet,
    update_mongo_db,
    mongo_update_command_for,
    copy_docs_in_update_cmd,
    df_from_sheet_in,
    _validate_changesheet,
)
from nmdc_runtime.site.ops import ensure_data_object_type
from nmdc_runtime.site.repository import run_config_frozen__normal_env
from nmdc_runtime.site.resources import get_mongo
from nmdc_runtime.util import REPO_ROOT_DIR

TEST_DATA_DIR = REPO_ROOT_DIR.joinpath("metadata-translation", "notebooks", "data")


@lru_cache
def load_studies() -> dict:
    studies = {}
    for i in (1, 2, 3):
        with open(TEST_DATA_DIR.joinpath(f"study-data{i}.json")) as f:
            s = json.load(f)
            studies[s["id"]] = s
    return studies


def get_study_by_id(id_: str) -> Optional[dict]:
    return load_studies().get(id_.strip())


def test_load_changesheet():
    mdb = get_mongo(run_config_frozen__normal_env).db
    sty_local_id = "sty-11-pzmd0x14"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + sty_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{sty_local_id}.json")
        ) as f:
            mdb.study_set_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    assert isinstance(df, pd.DataFrame)
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + sty_local_id})


def test_changesheet_update_slot_with_range_decimal():
    mdb = get_mongo_db()
    bsm_local_id = "bsm-11-0pyv7738"
    remove_tmp_doc = False
    if mdb.biosample_set.find_one({"id": "nmdc:" + bsm_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{bsm_local_id}.json")
        ) as f:
            mdb.biosample_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath("tests", "files", "test_changesheet_decimal_value.tsv"),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.biosample_set.delete_one({"id": "nmdc:" + bsm_local_id})


def test_changesheet_update_slot_with_range_bytes():
    mdb = get_mongo_db()
    dobj_local_id = "dobj-11-000n1286"
    remove_tmp_doc = False
    if mdb.data_object_set.find_one({"id": "nmdc:" + dobj_local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{dobj_local_id}.json")
        ) as f:
            mdb.data_object_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath(
            "tests", "files", "test_changesheet_update_bytes_ranged_slot.tsv"
        ),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.data_object_set.delete_one({"id": "nmdc:" + dobj_local_id})


def test_changesheet_update_slot_with_range_uriorcurie():
    mdb = get_mongo_db()
    local_id = "sty-11-pzmd0x14"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath("tests", "files", f"nmdc_{local_id}.json")
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        REPO_ROOT_DIR.joinpath(
            "tests", "files", "test_changesheet_insert_study_doi.tsv"
        ),
        mdb,
    )
    _validate_changesheet(df, mdb)
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})


@pytest.mark.skip(reason="no /site-packages/nmdc_schema/external_identifiers.yaml ?")
def test_update_01():
    mdb = get_mongo(run_config_frozen__normal_env).db
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-without-separator3.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    pi_info = {"has_raw_value": "NEW RAW NAME 2", "name": "NEW PI NAME 2"}
    assert study_doc["principal_investigator"] != pi_info
    assert study_doc["name"] != "NEW STUDY NAME 2"
    assert study_doc["ecosystem"] != "NEW ECOSYSTEM 2"
    assert study_doc["ecosystem_type"] != "NEW ECOSYSTEM_TYPE 2"
    assert study_doc["ecosystem_subtype"] != "NEW ECOSYSTEM_SUBTYPE 2"

    website_info = ["HTTP://TEST4.EXAMPLE.COM"]
    for website in website_info:
        assert website not in study_doc.get("websites", [])

    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_result = results[0]
    assert first_result["update_info"]["nModified"] == 11
    assert first_result["doc_after"]["principal_investigator"] == pi_info
    assert first_result["doc_after"]["name"] == "NEW STUDY NAME 2"
    assert first_result["doc_after"]["ecosystem"] == "NEW ECOSYSTEM 2"
    assert first_result["doc_after"]["ecosystem_type"] == "NEW ECOSYSTEM_TYPE 2"
    assert first_result["doc_after"]["ecosystem_subtype"] == "NEW ECOSYSTEM_SUBTYPE 2"
    for website in website_info:
        assert website in first_result["doc_after"]["websites"]
    assert first_result["validation_errors"] == []


def test_changesheet_array_item_nested_attributes():
    mdb = get_mongo(run_config_frozen__normal_env).db
    local_id = "sty-11-r2h77870"
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath(
                "tests", "files", f"study_no_credit_associations.json"
            )
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-array-item-nested-attributes.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    credit_info = {
        "applied_roles": ["Conceptualization"],
        "applies_to_person": {
            "name": "CREDIT NAME 1",
            "email": "CREDIT_NAME_1@foo.edu",
            "orcid": "orcid:0000-0000-0000-0001",
        },
    }
    assert credit_info not in study_doc.get("has_credit_associations", [])

    update_cmd = mongo_update_command_for(df)
    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_doc_after = results[0]["doc_after"]
    assert "has_credit_associations" in first_doc_after
    assert credit_info in first_doc_after.get("has_credit_associations", [])
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})


def test_update_pi_websites():
    mdb = get_mongo(run_config_frozen__normal_env).db
    local_id = "sty-11-r2h77870"
    restore_original_doc = False
    remove_tmp_doc = False
    if mdb.study_set.find_one({"id": "nmdc:" + local_id}) is None:
        with open(
            REPO_ROOT_DIR.joinpath(
                "tests", "files", f"study_no_credit_associations.json"
            )
        ) as f:
            mdb.study_set.insert_one(json.load(f))
            remove_tmp_doc = True
    else:
        restore_original_doc = True
    df = load_changesheet(
        TEST_DATA_DIR.joinpath("changesheet-update-pi-websites.tsv"), mdb
    )
    id_ = list(df.groupby("group_id"))[0][0]
    study_doc = dissoc(mdb.study_set.find_one({"id": id_}), "_id")

    pi_info = {
        "has_raw_value": "NEW PI NAME",
        "name": "NEW PI NAME",
        "profile_image_url": "https://portal.nersc.gov/NEW-PI-NAME.jpg",
        "orcid": "orcid:0000-0000-0000-0000",
        "websites": ["https://www.ornl.gov/staff-profile/NEW-PI-NAME"],
    }
    assert study_doc.get("principal_investigator", []) != pi_info

    update_cmd = mongo_update_command_for(df)

    mdb_scratch = mdb.client["nmdc_runtime_test"]
    copy_docs_in_update_cmd(
        update_cmd, mdb_from=mdb, mdb_to=mdb_scratch, drop_mdb_to=True
    )
    results = update_mongo_db(mdb_scratch, update_cmd)
    first_result_pi_info = results[0]["doc_after"]["principal_investigator"]
    for k, v in pi_info.items():
        assert first_result_pi_info[k] == v
    if remove_tmp_doc:
        mdb.study_set.delete_one({"id": "nmdc:" + local_id})
    if restore_original_doc:
        mdb.study_set.replace_one({"id": id_}, study_doc)


def test_update_biosample_ph():
    mdb = get_mongo_db()
    doc = json.loads(
        (REPO_ROOT_DIR / "tests" / "files" / "nmdc_bsm-11-5nhz3402.json").read_text()
    )
    mdb.biosample_set.replace_one({"id": "nmdc:bsm-11-5nhz3402"}, doc, upsert=True)
    df = load_changesheet(
        (REPO_ROOT_DIR / "tests" / "files" / "test_changesheet_update_one_ph.tsv"), mdb
    )

    update_cmd = mongo_update_command_for(df)

    assert isinstance(
        update_cmd["nmdc:bsm-11-5nhz3402"]["updates"][0]["u"]["$set"]["ph"], float
    )


def test_ensure_data_object_type():
    docs_test = {
        "data_object_set": [
            {
                "description": "Protein FAA for gold:Gp0116326",
                "url": "https://data.microbiomedata.org/data/nmdc:mga06z11/annotation/nmdc_mga06z11_proteins.faa",
                "md5_checksum": "87733039aa2ef02667987b398b8df08c",
                "type": "nmdc:DataObject",
                "file_size_bytes": 1214244683,
                "id": "nmdc:87733039aa2ef02667987b398b8df08c",
                "name": "gold:Gp0116326_Protein FAA",
            }
        ]
    }
    mdb = get_mongo(run_config_frozen__normal_env).db
    docs, _ = ensure_data_object_type(docs_test, mdb)
    nmdc_jsonschema = get_nmdc_jsonschema_dict(enforce_id_patterns=False)
    nmdc_jsonschema["$defs"]["FileTypeEnum"]["enum"] = mdb.file_type_enum.distinct("id")
    nmdc_jsonschema_validator = fastjsonschema.compile(nmdc_jsonschema)

    _ = nmdc_jsonschema_validator(docs)  # raises JsonSchemaValueException if wrong
