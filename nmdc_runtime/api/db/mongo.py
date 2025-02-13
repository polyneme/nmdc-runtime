import gzip
import json
import os
from collections import defaultdict
from contextlib import AbstractContextManager
from functools import lru_cache
from typing import Set, Dict, Any, Iterable
from uuid import uuid4

import bson
from linkml_runtime import SchemaView
from nmdc_schema.get_nmdc_view import ViewGetter
from nmdc_schema.nmdc_data import get_nmdc_schema_definition
from pymongo.errors import OperationFailure, AutoReconnect
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pydantic import BaseModel, conint
from tenacity import wait_random_exponential, retry, retry_if_exception_type
from toolz import concat, merge, unique, dissoc

from nmdc_runtime.config import DATABASE_CLASS_NAME
from nmdc_runtime.util import (
    get_nmdc_jsonschema_dict,
    schema_collection_names_with_id_field,
    nmdc_schema_view,
    collection_name_to_class_names,
)
from pymongo import MongoClient, ReplaceOne
from pymongo.database import Database as MongoDatabase


@retry(
    retry=retry_if_exception_type(AutoReconnect),
    wait=wait_random_exponential(multiplier=0.5, max=60),
)
def check_mongo_ok_autoreconnect(mdb: MongoDatabase):
    mdb["_runtime.healthcheck"].insert_one({"_id": "ok"})
    mdb["_runtime.healthcheck"].delete_one({"_id": "ok"})
    return True


@lru_cache
def get_mongo_db() -> MongoDatabase:
    _client = MongoClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    mdb = _client[os.getenv("MONGO_DBNAME")]
    check_mongo_ok_autoreconnect(mdb)
    return mdb


@lru_cache
def get_async_mongo_db() -> AsyncIOMotorDatabase:
    _client = AsyncIOMotorClient(
        host=os.getenv("MONGO_HOST"),
        username=os.getenv("MONGO_USERNAME"),
        password=os.getenv("MONGO_PASSWORD"),
        directConnection=True,
    )
    return _client[os.getenv("MONGO_DBNAME")]


def get_nonempty_nmdc_schema_collection_names(mdb: MongoDatabase) -> Set[str]:
    """Returns the names of schema collections in the database that have at least one document."""
    names = set(mdb.list_collection_names()) & set(get_collection_names_from_schema())
    return {name for name in names if mdb[name].estimated_document_count() > 0}


@lru_cache
def get_collection_names_from_schema() -> list[str]:
    """
    Returns the names of the slots of the `Database` class that describe database collections.

    Source: https://github.com/microbiomedata/refscan/blob/af092b0e068b671849fe0f323fac2ed54b81d574/refscan/lib/helpers.py#L31
    """
    collection_names = []

    schema_view = nmdc_schema_view()
    for slot_name in schema_view.class_slots(DATABASE_CLASS_NAME):
        slot_definition = schema_view.induced_slot(slot_name, DATABASE_CLASS_NAME)

        # Filter out any hypothetical (future) slots that don't correspond to a collection (e.g. `db_version`).
        if slot_definition.multivalued and slot_definition.inlined_as_list:
            collection_names.append(slot_name)

        # Filter out duplicate names. This is to work around the following issues in the schema:
        # - https://github.com/microbiomedata/nmdc-schema/issues/1954
        # - https://github.com/microbiomedata/nmdc-schema/issues/1955
        collection_names = list(set(collection_names))

    return collection_names


@lru_cache
def activity_collection_names(mdb: MongoDatabase) -> Set[str]:
    return get_nonempty_nmdc_schema_collection_names(mdb) - {
        "biosample_set",
        "study_set",
        "data_object_set",
        "functional_annotation_set",
        "genome_feature_set",
    }


@lru_cache
def get_planned_process_collection_names() -> Set[str]:
    r"""
    Returns the names of all collections that the schema says can contain documents
    that represent instances of the `PlannedProcess` class or any of its subclasses.
    """
    schema_view = nmdc_schema_view()
    collection_names = set()
    planned_process_descendants = set(schema_view.class_descendants("PlannedProcess"))

    for collection_name, class_names in collection_name_to_class_names.items():
        for class_name in class_names:
            # If the name of this class is the name of the `PlannedProcess` class
            # or any of its subclasses, add it to the result set.
            if class_name in planned_process_descendants:
                collection_names.add(collection_name)

    return collection_names


def mongodump_excluded_collections():
    _mdb = get_mongo_db()
    excluded_collections = " ".join(
        f"--excludeCollection={c}"
        for c in sorted(
            set(_mdb.list_collection_names()) - set(get_collection_names_from_schema())
        )
    )
    return excluded_collections


def mongorestore_collection(mdb, collection_name, bson_file_path):
    """
    Replaces the specified collection with one that reflects the contents of the
    specified BSON file.
    """
    with gzip.open(bson_file_path, "rb") as bson_file:
        data = bson.decode_all(bson_file.read())
        if data:
            mdb.drop_collection(collection_name)
            mdb[collection_name].insert_many(data)
            print(
                f"mongorestore_collection: inserted {len(data)} documents into {collection_name} after drop"
            )
        else:
            print(f"mongorestore_collection: no {collection_name} documents found")


def mongorestore_from_dir(mdb, dump_directory, skip_collections=None):
    """
    Effectively runs a `mongorestore` command in pure Python.
    Helpful in a container context that does not have the `mongorestore` command available.
    """
    skip_collections = skip_collections or []
    for root, dirs, files in os.walk(dump_directory):
        for file in files:
            if file.endswith(".bson.gz"):
                collection_name = file.replace(".bson.gz", "")
                if collection_name in skip_collections:
                    continue
                bson_file_path = os.path.join(root, file)
                mongorestore_collection(mdb, collection_name, bson_file_path)

    print("mongorestore_from_dir completed successfully.")
