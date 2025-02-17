import datetime
import json
from typing import Optional, Any, Dict, List, Union

import bson
import bson.json_util
from pydantic import (
    model_validator,
    Field,
    BaseModel,
    PositiveInt,
    NonNegativeInt,
    field_validator,
    ConfigDict,
    WrapSerializer,
)
from pymongo.database import Database as MongoDatabase
from toolz import assoc
from typing_extensions import Annotated

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now, pick
from nmdc_runtime.api.db.mongo import get_mongo_db


def bson_to_json(doc: Any, handler) -> dict:
    """Ensure a dict with e.g. mongo ObjectIds will serialize as JSON."""
    return json.loads(bson.json_util.dumps(doc))


Document = Annotated[Dict[str, Any], WrapSerializer(bson_to_json)]

OneOrZero = Annotated[int, Field(ge=0, le=1)]
One = Annotated[int, Field(ge=1, le=1)]
MinusOne = Annotated[int, Field(ge=-1, le=-1)]
OneOrMinusOne = Union[One, MinusOne]


class CommandBase(BaseModel):
    comment: Optional[Any] = None


class CollStatsCommand(CommandBase):
    collStats: str
    scale: Optional[int] = 1


class CountCommand(CommandBase):
    count: str
    query: Optional[Document] = None


class FindCommand(CommandBase):
    find: str
    filter: Optional[Document] = None
    projection: Optional[Dict[str, OneOrZero]] = None
    allowPartialResults: Optional[bool] = True
    batchSize: Optional[PositiveInt] = 101
    sort: Optional[Dict[str, OneOrMinusOne]] = None
    limit: Optional[NonNegativeInt] = None


class AggregateCommand(CommandBase):
    aggregate: str
    pipeline: List[Document]
    allowDiskUse: Optional[bool] = False
    cursor: Optional[Document] = None

    @field_validator("pipeline")
    @classmethod
    def disallow_invalid_pipeline_stages(
        cls, pipeline: List[Document]
    ) -> List[Document]:
        deny_list = ["$out", "$merge"]

        if any(
            key in deny_list for pipeline_stage in pipeline for key in pipeline_stage
        ):
            raise ValueError("$Out and $merge pipeline stages are not allowed.")

        return pipeline

    @model_validator(mode="before")
    @classmethod
    def ensure_default_value_for_cursor(cls, data: Any) -> Document:
        if isinstance(data, dict) and "cursor" not in data:
            return assoc(data, "cursor", {"batchSize": 25})
        return data


class GetMoreCommand(CommandBase):
    # Note: No `collection` field. See `CursorContinuation` for inter-API-request "sessions" are modeled.
    getMore: str  # Note: runtime uses a `str` id, not an `int` like mongo's native session cursors.
    batchSize: Optional[PositiveInt] = None


class CommandResponse(BaseModel):
    query_id: str
    ran_at: datetime.datetime
    ok: OneOrZero


class CollStatsCommandResponse(CommandResponse):
    ns: str
    size: float
    count: float
    avgObjSize: Optional[float] = None
    storageSize: float
    totalIndexSize: float
    totalSize: float
    scaleFactor: float


class CountCommandResponse(CommandResponse):
    n: NonNegativeInt


class CommandResponseCursor(BaseModel):
    # Note: No `ns` field, `id` is a `str`, and `partialResultsReturned` aliased to `queriedShardsUnavailable` to be
    # less confusing to Runtime API clients. See `CursorContinuation` for inter-API-request "sessions" are modeled.
    partialResultsReturned: Optional[bool] = Field(
        None, alias="queriedShardsUnavailable"
    )
    id: Optional[str] = None

    @field_validator("id", mode="before")
    @classmethod
    def coerce_int_to_str(cls, value: Any) -> Any:
        if isinstance(value, int):
            return str(value)
        else:
            return value


class InitialCommandResponseCursor(CommandResponseCursor):
    firstBatch: List[Document]


class GetMoreCommandResponseCursor(CommandResponseCursor):
    nextBatch: List[Document]


class GetMoreCommandResponse(CommandResponse):
    cursor: GetMoreCommandResponseCursor


class FindOrAggregateCommandResponse(CommandResponse):
    cursor: InitialCommandResponseCursor

    @classmethod
    def cursor_batch__ids_only(cls, cmd_response) -> "FindOrAggregateCommandResponse":
        """Create a new response object that retains only the `_id` for each cursor firstBatch|nextBatch document."""
        doc: dict = cmd_response.model_dump(exclude_unset=True)
        # FIXME
        batch_key = "firstBatch" if "firstBatch" in doc["cursor"] else "nextBatch"
        doc["cursor"]["firstBatch"] = [
            pick(["_id"], batch_doc) for batch_doc in doc["cursor"][batch_key]
        ]
        if batch_key == "nextBatch":
            del doc["cursor"]["nextBatch"]
        return cls(**doc)


class DeleteStatement(BaseModel):
    q: Document
    limit: OneOrZero
    hint: Optional[Dict[str, OneOrMinusOne]] = None


class DeleteCommand(CommandBase):
    delete: str
    deletes: List[DeleteStatement]


class DeleteCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    writeErrors: Optional[List[Document]] = None


# If `multi==True` all documents that meet the query criteria will be updated.
# Else only a single document that meets the query criteria will be updated.
class UpdateStatement(BaseModel):
    q: Document
    u: Document
    upsert: bool = False
    multi: bool = False
    hint: Optional[Dict[str, OneOrMinusOne]] = None


class UpdateCommand(CommandBase):
    update: str
    updates: List[UpdateStatement]


class DocumentUpserted(BaseModel):
    index: NonNegativeInt
    _id: bson.ObjectId


class UpdateCommandResponse(CommandResponse):
    ok: OneOrZero
    n: NonNegativeInt
    nModified: NonNegativeInt
    upserted: Optional[List[DocumentUpserted]] = None
    writeErrors: Optional[List[Document]] = None


QueryCmd = Union[
    CollStatsCommand,
    CountCommand,
    FindCommand,
    GetMoreCommand,
    DeleteCommand,
    UpdateCommand,
    AggregateCommand,
]

QueryResponseOptions = Union[
    CollStatsCommandResponse,
    CountCommandResponse,
    FindOrAggregateCommandResponse,
    GetMoreCommandResponse,
    DeleteCommandResponse,
    UpdateCommandResponse,
]

CursorCommand = Union[
    AggregateCommand,
    FindCommand,
    GetMoreCommand,
]

CursorResponse = Union[
    FindOrAggregateCommandResponse,
    GetMoreCommandResponse,
]


def command_response_for(type_):
    d = {
        CollStatsCommand: CollStatsCommandResponse,
        CountCommand: CountCommandResponse,
        FindCommand: FindOrAggregateCommandResponse,
        GetMoreCommand: GetMoreCommandResponse,
        DeleteCommand: DeleteCommandResponse,
        UpdateCommand: UpdateCommandResponse,
        AggregateCommand: FindOrAggregateCommandResponse,
    }
    return d.get(type_)


_mdb = get_mongo_db()


class Query(BaseModel):
    id: str
    cmd: QueryCmd

    @classmethod
    def from_cmd(cls, cmd: QueryCmd) -> "Query":
        return Query(cmd=cmd, id=generate_one_id(_mdb, "qy"))
