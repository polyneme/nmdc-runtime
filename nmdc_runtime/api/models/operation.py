import datetime
from typing import Generic, TypeVar, Optional, List, Any, Union

from pydantic import BaseModel, validator, ValidationError, HttpUrl, constr
from pydantic.generics import GenericModel

ResultT = TypeVar("ResultT")
MetadataT = TypeVar("MetadataT")


PythonImportPath = constr(regex=r"^[A-Za-z0-9_.]+$")


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


class UpdateOperationRequest(GenericModel, Generic[ResultT, MetadataT]):
    done: bool = False
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
    model: Optional[PythonImportPath]


class EmptyResult(Result):
    pass


class Metadata(BaseModel):
    # TODO set model field using __class__ on construct?
    model: Optional[PythonImportPath]


class PausedOrNot(Metadata):
    paused: bool


class ObjectPutMetadata(Metadata):
    object_id: str
    site_id: str
    url: HttpUrl
    expires_in_seconds: int
