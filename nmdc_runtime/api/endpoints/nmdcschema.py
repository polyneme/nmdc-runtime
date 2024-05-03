from importlib.metadata import version

import pymongo
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON
from bson import json_util
from fastapi import APIRouter, Depends, HTTPException

from nmdc_runtime.minter.config import typecodes
from nmdc_runtime.util import (
    nmdc_database_collection_names,
    collection_name_to_class_names,
)
from pymongo.database import Database as MongoDatabase
from starlette import status
from toolz import dissoc

from nmdc_runtime.api.core.metadata import map_id_to_collection, get_collection_for_id
from nmdc_runtime.api.core.util import raise404_if_none
from nmdc_runtime.api.db.mongo import get_mongo_db, nmdc_schema_collection_names
from nmdc_runtime.api.endpoints.util import (
    list_resources,
    check_filter,
    FUSEKI_HOST,
    FUSEKI_USER,
    FUSEKI_PASSWD,
    comma_separated_values,
)
from nmdc_runtime.api.models.metadata import Doc
from nmdc_runtime.api.models.util import ListRequest, ListResponse, AssociationsRequest

router = APIRouter()


def verify_collection_name(
    collection_name: str, mdb: MongoDatabase = Depends(get_mongo_db)
):
    names = nmdc_schema_collection_names(mdb)
    if collection_name not in names:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Collection name must be one of {names}",
        )


def strip_oid(doc):
    return dissoc(doc, "_id")


@router.get("/nmdcschema/version")
def get_nmdc_schema_version():
    """
    To view the [NMDC Schema](https://microbiomedata.github.io/nmdc-schema/) version the database is currently using,
    try executing the GET /nmdcschema/version endpoint
    """
    return version("nmdc_schema")


@router.get("/nmdcschema/typecodes")
def get_nmdc_schema_typecodes():
    return typecodes()


@router.get("/nmdcschema/collection_stats")
def get_nmdc_database_collection_stats(
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    To get the NMDC Database MongoDB collection statistics, like the total count of records in a collection or the size
    of the collection, try executing the GET /nmdcschema/collection_stats endpoint

    Field reference: <https://www.mongodb.com/docs/manual/reference/command/collStats/#std-label-collStats-output>.
    """
    # Take set intersection of
    #   (1) all collections defined by the NMDC schema, and
    #   (2) all runtime collections
    # Thus, only retrieve collections from the schema that are present (i.e. having actual documents) in the runtime.
    present_collection_names = set(nmdc_database_collection_names()) & set(
        mdb.list_collection_names()
    )
    stats = []
    for n in present_collection_names:
        for doc in mdb[n].aggregate(
            [
                {"$collStats": {"storageStats": {}}},
                {
                    "$project": {
                        "ns": 1,
                        "storageStats.size": 1,
                        "storageStats.count": 1,
                        "storageStats.avgObjSize": 1,
                        "storageStats.storageSize": 1,
                        "storageStats.totalIndexSize": 1,
                        "storageStats.totalSize": 1,
                        "storageStats.scaleFactor": 1,
                    }
                },
            ]
        ):
            stats.append(doc)
    return stats


@router.get("/nmdcschema/associations")
def get_nmdc_schema_associations(
    req: AssociationsRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    For a given focus node of type nmdc:`start_type` with id `start_id`,
    find target nodes of type nmdc:`target_type`.

    You should not use the Swagger UI for values of `limit` much larger than `1000`.
    Set `limit` to `0` (zero) for no limit.
    """
    start_type_collection_name, target_type_collection_name = None, None
    for k, v in collection_name_to_class_names.items():
        if req.start_type in v:
            start_type_collection_name = k
        if req.target_type in v:
            target_type_collection_name = k
    if start_type_collection_name is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f'start_type "{req.start_type}" is not a known nmdc-schema class',
        )
    if target_type_collection_name is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f'target_type "{req.target_type}" is not a known nmdc-schema class',
        )

    start_node = mdb[start_type_collection_name].find_one({"id": req.start_id})
    if start_node is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=f'start_id "{req.start_id}" not found.',
        )

    values_stmt = f"VALUES ?start_node {{ {start_node['id']} }}"
    start_pattern = f"?start_node nmdc:type nmdc:{req.start_type} ."
    target_pattern = f"?o nmdc:type nmdc:{req.target_type} ."
    objects_that_can_reach_start_node_pattern = "?o nmdc:depends_on+ ?start_node ."
    objects_reachable_from_start_node_pattern = "?start_node nmdc:depends_on+ ?o ."
    where_clause = f"""
        {values_stmt} {start_pattern} {target_pattern}
        {{
          {{ {objects_that_can_reach_start_node_pattern} }}
          UNION
          {{ {objects_reachable_from_start_node_pattern} }}
        }}
    """
    limit = f"LIMIT {req.limit}" if req.limit != 0 else ""
    query = f"""
    PREFIX nmdc: <https://w3id.org/nmdc/>
    SELECT DISTINCT ?o WHERE {{ 
        {where_clause}
    }} {limit}"""
    print(query)

    sparql = SPARQLWrapper(f"{FUSEKI_HOST}/nmdc")
    sparql.user = FUSEKI_USER
    sparql.passwd = FUSEKI_PASSWD
    sparql.setReturnFormat(SPARQL_JSON)
    sparql.setQuery(query)
    try:
        ret = sparql.queryAndConvert()
        target_ids = [
            b["o"]["value"].replace("https://w3id.org/nmdc/", "nmdc:")
            for b in ret["results"]["bindings"]
        ]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=str(e),
        )
    target_filter = (
        json_util.loads(check_filter(req.target_filter)) if req.target_filter else {}
    )
    if target_ids:
        target_filter["id"] = {"$in": target_ids}
        print(target_filter)
        print(target_type_collection_name)
        target_docs = list_resources(
            ListRequest(
                filter=json_util.dumps(target_filter),
                projection=req.target_projection,
                max_page_size=0,
            ),
            mdb,
            target_type_collection_name,
        )
        print(target_docs)
        return [strip_oid(d) for d in target_docs["resources"]]
    else:
        return []


@router.get(
    "/nmdcschema/{collection_name}",
    response_model=ListResponse[Doc],
    response_model_exclude_unset=True,
    dependencies=[Depends(verify_collection_name)],
)
def list_from_collection(
    collection_name: str,
    req: ListRequest = Depends(),
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    The GET /nmdcschema/{collection_name} endpoint is a general purpose way to retrieve metadata about a specified
    collection given user-provided filter and projection criteria. Please see the [Collection Names](https://microbiomedata.github.io/nmdc-schema/Database/)
    that may be retrieved. Please note that metadata may only be retrieved about one collection at a time.
    """
    rv = list_resources(req, mdb, collection_name)
    rv["resources"] = [strip_oid(d) for d in rv["resources"]]
    return rv


@router.get(
    "/nmdcschema/ids/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
)
def get_by_id(
    doc_id: str,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    If the identifier of the record is known, the GET /nmdcshema/ids/{doc_id} can be used to retrieve the specified record.
    \n Note that only one identifier may be used at a time, and therefore, only one record may be retrieved at a time using this method.
    """
    id_dict = map_id_to_collection(mdb)
    collection_name = get_collection_for_id(doc_id, id_dict)
    return strip_oid(
        raise404_if_none(
            collection_name and (mdb[collection_name].find_one({"id": doc_id}))
        )
    )


@router.get(
    "/nmdcschema/{collection_name}/{doc_id}",
    response_model=Doc,
    response_model_exclude_unset=True,
    dependencies=[Depends(verify_collection_name)],
)
def get_from_collection_by_id(
    collection_name: str,
    doc_id: str,
    projection: str | None = None,
    mdb: MongoDatabase = Depends(get_mongo_db),
):
    """
    If both the identifier and the collection name of the desired record is known, the
    GET /nmdcschema/{collection_name}/{doc_id} can be used to retrieve the record. The projection parameter is optionally
    available for this endpoint to retrieve only desired attributes from a record. Please note that only one record can
    be retrieved at one time using this method.

    for MongoDB-like [projection](https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/): comma-separated list of fields you want the objects in the response to include. Example: `id,doi`
    """
    projection = projection.split(",") if projection else None
    try:
        return strip_oid(
            raise404_if_none(
                mdb[collection_name].find_one({"id": doc_id}, projection=projection)
            )
        )
    except pymongo.errors.OperationFailure as e:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(e)
        )
