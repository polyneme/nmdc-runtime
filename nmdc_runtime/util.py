import json
import mimetypes
import os
import pkgutil
from collections.abc import Iterable
from contextlib import AbstractContextManager
from copy import deepcopy
from datetime import datetime, timezone
from functools import lru_cache
from io import BytesIO
from itertools import chain
from pathlib import Path
from uuid import uuid4
from typing import List, Optional, Set, Dict

import fastjsonschema
import requests
from frozendict import frozendict
from jsonschema.validators import Draft7Validator
from nmdc_schema.nmdc import Database as NMDCDatabase
from nmdc_schema.get_nmdc_view import ViewGetter
from pydantic import Field, BaseModel
from pymongo.database import Database as MongoDatabase
from pymongo.errors import OperationFailure
from refscan.lib.helpers import (
    derive_schema_class_name_from_document,
    identify_references,
)
from refscan.lib.Finder import Finder
from refscan.lib.ReferenceList import ReferenceList
from refscan.lib.Violation import Violation
from toolz import merge, unique

from nmdc_runtime.api.core.util import sha256hash_from_file
from nmdc_runtime.api.models.object import DrsObjectIn
from typing_extensions import Annotated


def get_class_names_from_collection_spec(
    spec: dict, prefix: Optional[str] = None
) -> List[str]:
    """
    Returns the list of classes referenced by the `$ref` values in a JSON Schema snippet describing a collection,
    applying an optional prefix to each class name.

    >>> get_class_names_from_collection_spec({"items": {"foo": "#/$defs/A"}})
    []
    >>> get_class_names_from_collection_spec({"items": {"$ref": "#/$defs/A"}})
    ['A']
    >>> get_class_names_from_collection_spec({"items": {"$ref": "#/$defs/A"}}, "p:")
    ['p:A']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": "not-a-list"}})
    []
    >>> get_class_names_from_collection_spec({"items": {"anyOf": []}})
    []
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}]}})
    ['A']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}]}})
    ['A', 'B']
    >>> get_class_names_from_collection_spec({"items": {"anyOf": [{"$ref": "#/$defs/A"}, {"$ref": "#/$defs/B"}]}}, "p:")
    ['p:A', 'p:B']
    """

    class_names = []
    if "items" in spec:
        # If the `items` dictionary has a key named `$ref`, get the single class name from it.
        if "$ref" in spec["items"]:
            ref_dict = spec["items"]["$ref"]
            class_name = ref_dict.split("/")[-1]  # e.g. `#/$defs/Foo` --> `Foo`
            class_names.append(class_name)

        # Else, if it has a key named `anyOf` whose value is a list, get the class name from each ref in the list.
        elif "anyOf" in spec["items"] and isinstance(spec["items"]["anyOf"], list):
            for element in spec["items"]["anyOf"]:
                ref_dict = element["$ref"]
                class_name = ref_dict.split("/")[-1]  # e.g. `#/$defs/Foo` --> `Foo`
                class_names.append(class_name)

    # Apply the specified prefix, if any, to each class name.
    if isinstance(prefix, str):
        class_names = list(map(lambda name: f"{prefix}{name}", class_names))

    return class_names


@lru_cache
def get_allowed_references() -> ReferenceList:
    r"""
    Returns a `ReferenceList` of all the inter-document references that
    the NMDC Schema allows a schema-compliant MongoDB database to contain.
    """

    # Identify the inter-document references that the schema allows a database to contain.
    print("Identifying schema-allowed references.")
    references = identify_references(
        schema_view=nmdc_schema_view(),
        collection_name_to_class_names=collection_name_to_class_names,
    )

    return references


@lru_cache
def get_type_collections() -> dict:
    """Returns a dictionary mapping class names to Mongo collection names."""

    mappings = {}

    # Process the `items` dictionary of each collection whose name ends with `_set`.
    for collection_name, spec in nmdc_jsonschema["properties"].items():
        if collection_name.endswith("_set"):
            class_names = get_class_names_from_collection_spec(spec, "nmdc:")
            for class_name in class_names:
                mappings[class_name] = collection_name

    return mappings


def without_id_patterns(nmdc_jsonschema):
    rv = deepcopy(nmdc_jsonschema)
    for cls_, spec in rv["$defs"].items():
        if "properties" in spec:
            if "id" in spec["properties"]:
                spec["properties"]["id"].pop("pattern", None)
    return rv


@lru_cache
def get_nmdc_jsonschema_dict(enforce_id_patterns=True):
    """Get NMDC JSON Schema with materialized patterns (for identifier regexes)."""
    d = json.loads(
        BytesIO(
            pkgutil.get_data("nmdc_schema", "nmdc_materialized_patterns.schema.json")
        )
        .getvalue()
        .decode("utf-8")
    )
    return d if enforce_id_patterns else without_id_patterns(d)


@lru_cache
def get_nmdc_jsonschema_validator(enforce_id_patterns=True):
    return fastjsonschema.compile(
        get_nmdc_jsonschema_dict(enforce_id_patterns=enforce_id_patterns)
    )


nmdc_jsonschema = get_nmdc_jsonschema_dict()
nmdc_jsonschema_validator = get_nmdc_jsonschema_validator()
nmdc_jsonschema_noidpatterns = get_nmdc_jsonschema_dict(enforce_id_patterns=False)
nmdc_jsonschema_validator_noidpatterns = get_nmdc_jsonschema_validator(
    enforce_id_patterns=False
)

REPO_ROOT_DIR = Path(__file__).parent.parent


def put_object(filepath, url, mime_type=None):
    if mime_type is None:
        mime_type = mimetypes.guess_type(filepath)[0]
    with open(filepath, "rb") as f:
        return requests.put(url, data=f, headers={"Content-Type": mime_type})


def drs_metadata_for(filepath, base=None, timestamp=None):
    """given file path, get drs metadata

    required: size, created_time, and at least one checksum.
    """
    base = {} if base is None else base
    if "size" not in base:
        base["size"] = os.path.getsize(filepath)
    if "created_time" not in base:
        base["created_time"] = datetime.fromtimestamp(
            os.path.getctime(filepath), tz=timezone.utc
        )
    if "checksums" not in base:
        base["checksums"] = [
            {"type": "sha256", "checksum": sha256hash_from_file(filepath, timestamp)}
        ]
    if "mime_type" not in base:
        base["mime_type"] = mimetypes.guess_type(filepath)[0]
    if "name" not in base:
        base["name"] = Path(filepath).name
    return base


def drs_object_in_for(filepath, op_doc, base=None):
    access_id = f'{op_doc["metadata"]["site_id"]}:{op_doc["metadata"]["object_id"]}'
    drs_obj_in = DrsObjectIn(
        **drs_metadata_for(
            filepath,
            merge(base or {}, {"access_methods": [{"access_id": access_id}]}),
        )
    )
    return json.loads(drs_obj_in.json(exclude_unset=True))


def freeze(obj):
    """Recursive function for dict → frozendict, set → frozenset, list → tuple.

    For example, will turn JSON data into a hashable value.
    """
    try:
        # See if the object is hashable
        hash(obj)
        return obj
    except TypeError:
        pass

    if isinstance(obj, (dict, frozendict)):
        return frozendict({k: freeze(obj[k]) for k in obj})
    elif isinstance(obj, (set, frozenset)):
        return frozenset({freeze(elt) for elt in obj})
    elif isinstance(obj, (list, tuple)):
        return tuple([freeze(elt) for elt in obj])

    msg = "Unsupported type: %r" % type(obj).__name__
    raise TypeError(msg)


def unfreeze(obj):
    """frozendict → dict, frozenset → set, tuple → list."""
    if isinstance(obj, (dict, frozendict)):
        return {k: unfreeze(v) for k, v in obj.items()}
    elif isinstance(obj, (set, frozenset)):
        return {unfreeze(elt) for elt in obj}
    elif isinstance(obj, (list, tuple)):
        return [unfreeze(elt) for elt in obj]
    else:
        return obj


def pluralize(singular, using, pluralized=None):
    """Pluralize a word for output.

    >>> pluralize("job", 1)
    'job'
    >>> pluralize("job", 2)
    'jobs'
    >>> pluralize("datum", 2, "data")
    'data'
    """
    return (
        singular
        if using == 1
        else (pluralized if pluralized is not None else f"{singular}s")
    )


def iterable_from_dict_keys(d, keys):
    for k in keys:
        yield d[k]


def flatten(d):
    """Flatten a nested JSON-able dict into a flat dict of dotted-pathed keys."""
    # assumes plain-json-able
    d = json.loads(json.dumps(d))

    # atomic values are already "flattened"
    if not isinstance(d, (dict, list)):
        return d

    out = {}
    for k, v in d.items():
        if isinstance(v, list):
            for i, elt in enumerate(v):
                if isinstance(elt, dict):
                    for k_inner, v_inner in flatten(elt).items():
                        out[f"{k}.{i}.{k_inner}"] = v_inner
                elif isinstance(elt, list):
                    raise ValueError("Can't handle lists in lists at this time")
                else:
                    out[f"{k}.{i}"] = elt
        elif isinstance(v, dict):
            for kv, vv in v.items():
                if isinstance(vv, dict):
                    for kv_inner, vv_inner in flatten(vv).items():
                        out[f"{k}.{kv}.{kv_inner}"] = vv_inner
                elif isinstance(vv, list):
                    raise ValueError("Can't handle lists in sub-dicts at this time")
                else:
                    out[f"{k}.{kv}"] = vv
        else:
            out[k] = v
    return out


def find_one(k_v: dict, entities: Iterable[dict]):
    """Find the first entity with key-value pair k_v, if any?

    >>> find_one({"id": "foo"}, [{"id": "foo"}])
    True
    >>> find_one({"id": "foo"}, [{"id": "bar"}])
    False
    """
    if len(k_v) > 1:
        raise Exception("Supports only one key-value pair")
    k = next(k for k in k_v)
    return next((e for e in entities if k in e and e[k] == k_v[k]), None)


@lru_cache
def nmdc_activity_collection_names():
    slots = []
    view = ViewGetter().get_view()
    acts = set(view.class_descendants("WorkflowExecutionActivity"))
    acts -= {"WorkflowExecutionActivity"}
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in acts:
            slots.append(slot)
    return slots


@lru_cache
def nmdc_schema_view():
    return ViewGetter().get_view()


@lru_cache
def nmdc_database_collection_instance_class_names():
    names = []
    view = nmdc_schema_view()
    all_classes = set(view.all_classes())
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in all_classes:
            names.append(rng)
    return names


@lru_cache
def nmdc_database_collection_names():
    names = []
    view = nmdc_schema_view()
    all_classes = set(view.all_classes())
    for slot in view.class_slots("Database"):
        rng = getattr(view.get_slot(slot), "range", None)
        if rng in all_classes:
            names.append(slot)
    return names


def all_docs_have_unique_id(coll) -> bool:
    first_doc = coll.find_one({}, ["id"])
    if first_doc is None or "id" not in first_doc:
        # short-circuit exit for empty collection or large collection via first-doc peek.
        return False

    total_count = coll.count_documents({})
    return (
        # avoid attempt to fetch large (>16mb) list of distinct IDs,
        # a limitation of collection.distinct(). Use aggregation pipeline
        # instead to compute on mongo server, using disk if necessary.
        next(
            coll.aggregate(
                [{"$group": {"_id": "$id"}}, {"$count": "n_unique_ids"}],
                allowDiskUse=True,
            )
        )["n_unique_ids"]
        == total_count
    )


def specialize_activity_set_docs(docs):
    validation_errors = {}
    type_collections = get_type_collections()
    if "activity_set" in docs:
        for doc in docs["activity_set"]:
            doc_type = doc["type"]
            try:
                collection_name = type_collections[doc_type]
            except KeyError:
                msg = (
                    f"activity_set doc {doc.get('id', '<id missing>')} "
                    f"has type {doc_type}, which is not in NMDC Schema. "
                    "Note: Case is sensitive."
                )
                if "activity_set" in validation_errors:
                    validation_errors["activity_set"].append(msg)
                else:
                    validation_errors["activity_set"] = [msg]
                continue

            if collection_name in docs:
                docs[collection_name].append(doc)
            else:
                docs[collection_name] = [doc]
        del docs["activity_set"]
    return docs, validation_errors


# Define a mapping from collection name to a list of class names allowable for that collection's documents.
collection_name_to_class_names: Dict[str, List[str]] = {
    collection_name: list(
        set(
            chain.from_iterable(
                nmdc_schema_view().class_descendants(cls_name)
                for cls_name in get_class_names_from_collection_spec(spec)
            )
        )
    )
    for collection_name, spec in nmdc_jsonschema["$defs"]["Database"][
        "properties"
    ].items()
}


def class_hierarchy_as_list(obj) -> list[str]:
    """
    get list of inherited classes for each concrete class
    """
    rv = []
    current_class = obj.__class__

    def recurse_through_bases(cls):
        if cls.__name__ == "YAMLRoot":
            return rv
        rv.append(cls.__name__)
        for base in cls.__bases__:
            recurse_through_bases(base)
        return rv

    return recurse_through_bases(current_class)


@lru_cache
def schema_collection_names_with_id_field() -> Set[str]:
    """
    Returns the set of collection names with which _any_ of the associated classes contains an `id` field.
    """

    target_collection_names = set()

    for collection_name, class_names in collection_name_to_class_names.items():
        for class_name in class_names:
            if "id" in nmdc_jsonschema["$defs"][class_name].get("properties", {}):
                target_collection_names.add(collection_name)
                break

    return target_collection_names


def populated_schema_collection_names_with_id_field(mdb: MongoDatabase) -> List[str]:
    collection_names = sorted(schema_collection_names_with_id_field())
    return [n for n in collection_names if mdb[n].find_one({"id": {"$exists": True}})]


def ensure_unique_id_indexes(mdb: MongoDatabase):
    """Ensure that any collections with an "id" field have an index on "id"."""
    candidate_names = (
        set(mdb.list_collection_names()) | schema_collection_names_with_id_field()
    )
    for collection_name in candidate_names:
        if collection_name.startswith("system."):  # reserved by mongodb
            continue

        if (
            collection_name in schema_collection_names_with_id_field()
            or all_docs_have_unique_id(mdb[collection_name])
        ):
            mdb[collection_name].create_index("id", unique=True)


class UpdateStatement(BaseModel):
    q: dict
    u: dict
    upsert: bool = False
    multi: bool = False


class DeleteStatement(BaseModel):
    q: dict
    limit: Annotated[int, Field(ge=0, le=1)] = 1


class OverlayDBError(Exception):
    pass


class OverlayDB(AbstractContextManager):
    """Provides a context whereby a base Database is overlaid with a temporary one.

    If you need to run basic simulations of updates to a base database,
    you don't want to actually commit transactions to the base database.

    For example, to insert or replace (matching on "id") many documents into a collection in order
    to then validate the resulting total set of collection documents, an OverlayDB writes to
    an overlay collection that "shadows" the base collection during a "find" query
    (the "merge_find" method of an OverlayDB object): if a document with `id0` is found in the
    overlay collection, that id is marked as "seen" and will not also be returned when
    subsequently scanning the (unmodified) base-database collection.

    Mongo "update" commands (as the "apply_updates" method) are simulated by first copying affected
    documents from a base collection to the overlay, and then applying the updates to the overlay,
    so that again, base collections are unmodified, and a "merge_find" call will produce a result
    *as if* the base collection(s) were modified.

    Mongo deletions (as the "delete" method) also copy affected documents from the base collection
    to the overlay collection, and flag them using the "_deleted" field. In this way, a `merge_find`
    call will match a relevant document given a suitable filter, and will mark the document's id
    as "seen" *without* returning the document. Thus, the result is as if the document were deleted.

    Usage:
    ````
    with OverlayDB(mdb) as odb:
        # do stuff, e.g. `odb.replace_or_insert_many(...)`
    ```
    """

    def __init__(self, mdb: MongoDatabase):
        self._bottom_db = mdb
        self._top_db = self._bottom_db.client.get_database(f"overlay-{uuid4()}")
        ensure_unique_id_indexes(self._top_db)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._bottom_db.client.drop_database(self._top_db.name)

    def get_collection(self, coll_name: str):
        r"""Returns a reference to the specified collection."""
        try:
            return self._top_db[coll_name]
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def replace_or_insert_many(self, coll_name, documents: list):
        try:
            self._top_db[coll_name].insert_many(documents)
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def apply_updates(self, coll_name, updates: list):
        """prepare overlay db and apply updates to it."""
        assert all(UpdateStatement(**us) for us in updates)
        for update_spec in updates:
            for bottom_doc in self._bottom_db[coll_name].find(update_spec["q"]):
                self._top_db[coll_name].insert_one(bottom_doc)
        try:
            self._top_db.command({"update": coll_name, "updates": updates})
        except OperationFailure as e:
            raise OverlayDBError(str(e.details))

    def delete(self, coll_name, deletes: list):
        """ "apply" delete command by flagging docs in overlay database"""
        assert all(DeleteStatement(**us) for us in deletes)
        for delete_spec in deletes:
            for bottom_doc in self._bottom_db[coll_name].find(
                delete_spec["q"], limit=delete_spec.get("limit", 1)
            ):
                bottom_doc["_deleted"] = True
                self._top_db[coll_name].insert_one(bottom_doc)

    def merge_find(self, coll_name, find_spec: dict):
        """Yield docs first from overlay and then from base db, minding deletion flags."""
        # ensure projection of "id" and "_deleted"
        if "projection" in find_spec:
            proj = find_spec["projection"]
            if isinstance(proj, dict):
                proj = merge(proj, {"id": 1, "_deleted": 1})
            elif isinstance(proj, list):
                proj = list(unique(proj + ["id", "_deleted"]))

        top_docs = self._top_db[coll_name].find(**find_spec)
        bottom_docs = self._bottom_db[coll_name].find(**find_spec)
        top_seen_ids = set()
        for doc in top_docs:
            if not doc.get("_deleted"):
                yield doc
            top_seen_ids.add(doc["id"])

        for doc in bottom_docs:
            if doc["id"] not in top_seen_ids:
                yield doc


def validate_json(in_docs: dict, mdb: MongoDatabase):
    r"""
    Checks whether the specified dictionary represents a valid instance of the `Database` class
    defined in the NMDC Schema.

    Example dictionary:
    {
        "biosample_set": [
            {"id": "nmdc:bsm-00-000001", ...},
            {"id": "nmdc:bsm-00-000002", ...}
        ],
        "study_set": [
            {"id": "nmdc:sty-00-000001", ...},
            {"id": "nmdc:sty-00-000002", ...}
        ]
    }
    """
    validator = Draft7Validator(get_nmdc_jsonschema_dict())
    docs = deepcopy(in_docs)
    validation_errors = {}

    known_coll_names = set(nmdc_database_collection_names())
    for coll_name, coll_docs in docs.items():
        if coll_name not in known_coll_names:
            if coll_name == "@type" and coll_docs in ("Database", "nmdc:Database"):
                continue
            else:
                validation_errors[coll_name] = [
                    f"'{coll_name}' is not a known schema collection name"
                ]
                continue

        errors = list(validator.iter_errors({coll_name: coll_docs}))
        validation_errors[coll_name] = [e.message for e in errors]
        if coll_docs:
            if not isinstance(coll_docs, list):
                validation_errors[coll_name].append("value must be a list")
            elif not all(isinstance(d, dict) for d in coll_docs):
                validation_errors[coll_name].append(
                    "all elements of list must be dicts"
                )
            if not validation_errors[coll_name]:
                try:
                    with OverlayDB(mdb) as odb:
                        odb.replace_or_insert_many(coll_name, coll_docs)

                        # Check the referential integrity of the replaced or inserted documents.
                        #
                        # Note: If documents being inserted into the _current_ collection
                        #       refer to documents being inserted into a _different_ collection
                        #       as part of the same `in_docs` argument, this check will _not_
                        #       find the latter documents.
                        #
                        # TODO: Enhance this referential integrity validation to account for the
                        #       total of all operations; not just a single collection's operations.
                        #
                        # Note: Much of this code was copy/pasted from refscan, at:
                        #       https://github.com/microbiomedata/refscan/blob/46daba3b3cd05ee6a8a91076515f737248328cdb/refscan/refscan.py#L286-L349
                        #
                        source_collection_name = coll_name  # creates an alias to accommodate the copy/pasted code
                        finder = Finder(
                            database=odb
                        )  # uses a generic name to accommodate the copy/pasted code
                        references = (
                            get_allowed_references()
                        )  # uses a generic name to accommodate the copy/pasted code
                        reference_field_names_by_source_class_name = (
                            references.get_reference_field_names_by_source_class_name()
                        )
                        for document in coll_docs:

                            # Get the document's schema class name so that we can interpret its fields accordingly.
                            source_class_name = derive_schema_class_name_from_document(
                                schema_view=nmdc_schema_view(),
                                document=document,
                            )

                            # Get the names of that class's fields that can contain references.
                            # Get the names of that class's fields that can contain references.
                            names_of_reference_fields = (
                                reference_field_names_by_source_class_name.get(
                                    source_class_name, []
                                )
                            )

                            # Check each field that both (a) exists in the document and (b) can contain a reference.
                            for field_name in names_of_reference_fields:
                                if field_name in document:

                                    # Determine which collections can contain the referenced document, based upon
                                    # the schema class of which this source document is an instance.
                                    target_collection_names = (
                                        references.get_target_collection_names(
                                            source_class_name=source_class_name,
                                            source_field_name=field_name,
                                        )
                                    )

                                    # Handle both the multi-value (array) and the single-value (scalar) case,
                                    # normalizing the value or values into a list of values in either case.
                                    if type(document[field_name]) is list:
                                        target_ids = document[field_name]
                                    else:
                                        target_id = document[field_name]
                                        target_ids = [
                                            target_id
                                        ]  # makes a one-item list

                                    for target_id in target_ids:
                                        name_of_collection_containing_target_document = finder.check_whether_document_having_id_exists_among_collections(
                                            collection_names=target_collection_names,
                                            document_id=target_id,
                                        )
                                        if (
                                            name_of_collection_containing_target_document
                                            is None
                                        ):
                                            violation = Violation(
                                                source_collection_name=source_collection_name,
                                                source_field_name=field_name,
                                                source_document_object_id=document.get(
                                                    "_id"
                                                ),
                                                source_document_id=document.get("id"),
                                                target_id=target_id,
                                                name_of_collection_containing_target=None,
                                            )
                                            violation_as_str = (
                                                f"Document '{violation.source_document_id}' "
                                                f"in collection '{violation.source_collection_name}' "
                                                f"has a field '{violation.source_field_name}' that "
                                                f"references a document having id "
                                                f"'{violation.target_id}', but the latter document "
                                                f"does not exist in any of the collections the "
                                                f"NMDC Schema says it can exist in."
                                            )
                                            raise OverlayDBError(violation_as_str)

                except OverlayDBError as e:
                    validation_errors[coll_name].append(str(e))

    if all(len(v) == 0 for v in validation_errors.values()):
        # Second pass. Try instantiating linkml-sourced dataclass
        in_docs.pop("@type", None)
        try:
            NMDCDatabase(**in_docs)
        except Exception as e:
            return {"result": "errors", "detail": str(e)}

        return {"result": "All Okay!"}
    else:
        return {"result": "errors", "detail": validation_errors}
