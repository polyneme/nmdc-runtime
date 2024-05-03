from enum import Enum
from typing import TypeVar, List, Optional, Generic, Annotated

from fastapi import Query

from pydantic import model_validator, Field, BaseModel, NonNegativeInt
from typing_extensions import Annotated

ResultT = TypeVar("ResultT")


class ListResponse(BaseModel, Generic[ResultT]):
    resources: List[ResultT]
    next_page_token: Optional[str] = None


class ListRequest(BaseModel):
    filter: Annotated[
        Optional[str],
        Query(
            description='MongoDB-style JSON filter document. Example: `{"ecosystem_type": "Freshwater"}`'
        ),
    ] = None
    max_page_size: Optional[int] = 20
    page_token: Optional[str] = None
    projection: Annotated[
        Optional[str],
        Query(
            description=(
                "for MongoDB-like "
                "[projection](https://www.mongodb.com/docs/manual/tutorial/project-fields-from-query-results/): "
                "comma-separated list of fields you want the objects in the response to include. "
                "Note: `id` will always be included. "
                "Example: `ecosystem_type,name`"
            )
        ),
    ] = None


PerPageRange = Annotated[int, Field(gt=0, le=2_000)]


class AssociationDirectionEnum(str, Enum):
    upstream = "upstream"
    downstream = "downstream"


class AssociationsRequest(BaseModel):
    start_type: str
    start_id: str
    target_type: str
    target_filter: Optional[str] = None
    target_projection: Optional[str] = None
    limit: NonNegativeInt = 5


class FindRequest(BaseModel):
    filter: Optional[str] = None
    search: Optional[str] = None
    sort: Optional[str] = None
    page: Optional[int] = None
    per_page: Optional[PerPageRange] = 25
    cursor: Optional[str] = None
    group_by: Optional[str] = None
    fields: Annotated[
        Optional[str],
        Query(
            description="comma-separated list of fields you want the objects in the response to include"
        ),
    ] = None

    @model_validator(mode="before")
    def set_page_if_cursor_unset(cls, values):
        page, cursor = values.get("page"), values.get("cursor")
        if page is not None and cursor is not None:
            raise ValueError("cannot use cursor- and page-based pagination together")
        if page is None and cursor is None:
            values["page"] = 1
        return values


class PipelineFindRequest(BaseModel):
    pipeline_spec: str
    description: str


class FindResponse(BaseModel):
    meta: dict
    results: List[dict]
    group_by: List[dict]


class PipelineFindResponse(BaseModel):
    meta: dict
    results: List[dict]


# Note: For MongoDB, a single collection can have no more than 64 indexes
# Note: Each collection has a unique index set on "id" elsewhere.
entity_attributes_to_index = {
    "biosample_set": {
        "alternative_identifiers",
        "env_broad_scale.has_raw_value",
        "env_local_scale.has_raw_value",
        "env_medium.has_raw_value",
        "collection_date.has_raw_value",
        "ecosystem",
        "ecosystem_category",
        "ecosystem_type",
        "ecosystem_subtype",
        "specific_ecosystem",
        # Note: if `lat_lon` was GeoJSON, i.e. {type,coordinates}, MongoDB has a "2dsphere" index
        "lat_lon.latitude",
        "lat_lon.longitude",
    },
    "study_set": {
        "has_credit_associations.applied_roles",
        "has_credit_associations.applies_to_person.name",
        "has_credit_associations.applies_to_person.orcid",
    },
    "data_object_set": {
        "data_object_type",
        "file_size_bytes",
        "md5_checksum",
        "url",
    },
    "omics_processing_set": {
        "has_input",
        "has_output",
        "instrument_name",
        "alternative_identifiers",
    },
}
