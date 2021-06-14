import datetime
from typing import Generic, TypeVar, Optional, List, Any, Union

from pydantic import BaseModel, validator, ValidationError, HttpUrl
from pydantic.generics import GenericModel

ResultT = TypeVar("ResultT")
MetadataT = TypeVar("MetadataT")


class OperationError(BaseModel):
    code: str
    message: str
    details: Any


class Operation(GenericModel, Generic[ResultT, MetadataT]):
    id: str
    done: bool = False
    expire_time: datetime.datetime
    result: Optional[Union[ResultT, OperationError]]
    metadata: Optional[MetadataT]


class ListOperationsRequest(BaseModel):
    filter: Optional[str]
    max_page_size: Optional[int] = 20
    page_token: Optional[str]


class ListOperationsResponse(GenericModel, Generic[ResultT, MetadataT]):
    resources: List[Operation[ResultT, MetadataT]]
    next_page_token: Optional[str]


class Result(BaseModel):
    pass


class EmptyResult(Result):
    pass


class Metadata(BaseModel):
    pass


class PausedOrNot(Metadata):
    paused: bool


class ObjectPutMetadata(Metadata):
    object_id: str
    site_id: str
    url: HttpUrl
    expires_in_seconds: int
