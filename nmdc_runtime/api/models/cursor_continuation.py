"""
A *cursor continuation* is a means to effectively resume a `find` or `aggregate` MongoDB database command.

A *cursor continuation* document represents a *continuation* (cf. <https://en.wikipedia.org/wiki/Continuation>) for a
command and uses a stored value ("cursor") for MongoDB's guaranteed unique-valued document field, `_id`,
such that the documents returned by the command are guaranteed to be sorted in ascending order by `_id`.

In this way, an API client may retrieve all documents defined by a `find` or `aggregate` command over multiple HTTP
requests. One can think of this process as akin to pagination; however, with "cursor-based" pagination, there are no
guarantees wrt a fixed "page size".

"""

import datetime
from typing import Union, List, Callable

from bson import ObjectId
from pydantic import BaseModel, Field
from pymongo.database import Database as MongoDatabase

from nmdc_runtime.api.core.idgen import generate_one_id
from nmdc_runtime.api.core.util import now
from nmdc_runtime.api.db.mongo import get_mongo_db
from nmdc_runtime.api.models.query import (
    FindCommand,
    AggregateCommand,
    GetMoreCommand,
    AggregateCommandResponse,
    FindCommandResponse,
    GetMoreCommandResponse,
    InitialCommandResponseCursor,
    GetMoreCommandResponseCursor,
    QueryRun,
    Query,
    CursorCommand,
    CursorResponse,
)
from nmdc_runtime.api.models.user import User

_mdb: MongoDatabase = get_mongo_db()

# Ensure one-hour TTL on `_runtime.cursor_continuations` documents via TTL Index.
_mdb["_runtime.cursor_continuations"].create_index(
    {"last_modified": 1}, expireAfterSeconds=3600
)

_coll_cc = _mdb["_runtime.cursor_continuations"]


class CursorContinuation(BaseModel):
    """Represents a sequence of command, response, command, response, etc. associated with a user.

    This model is intended to correspond to a logical user "session" over several HTTP requests, and may be discarded
    after a user has retrieved all "batches" of documents. Thus, a collection of cursor continuations may be
    reasonably given e.g. a so-called "TTL Index" for the `last_modified` field in MongoDB, assuming that
    `last_modified` is updated each time `query_runs` is extended.
    """

    query_runs: List[QueryRun]
    id: str = Field(..., alias="_id")
    user: User
    last_modified: datetime.datetime


def _dump(m: BaseModel):
    return m.model_dump(by_alias=True, exclude_unset=True)


def create_cc(cursor_command: dict, user: dict):
    if "find" in cursor_command:
        query_cmd = FindCommand(**cursor_command)
    elif "aggregate" in cursor_command:
        query_cmd = AggregateCommand(**cursor_command)
    elif "getMore" in cursor_command:
        query_cmd = GetMoreCommand(**cursor_command)
    else:
        raise ValueError("Unknown cursor command")

    query = Query.from_cmd(query_cmd)
    cc = CursorContinuation(
        history=[query_cmd],
        _id=generate_one_id(_mdb, "cursor_continuations"),
        user=User(**user),
        last_modified=now(),
    )
    doc = _dump(cc)
    _coll_cc.insert_one(doc)
    return doc


def get_cc_by_id(cc_id: str):
    doc = _coll_cc.find_one({"_id": cc_id})
    if doc is not None:
        return _dump(CursorContinuation(**doc))


def get_more_with_cursor_continuation(cc_id: str):
    """Return next batch of documents pointed to by the cursor continuation."""
    doc = get_cc_by_id(cc_id)
    if doc is None:
        return Exception(f"Continuation {cc_id} Not Found")
    cc = CursorContinuation(**doc)
    last_history_item = cc.history[-1]
    if isinstance(last_history_item, CursorCommand):
        return Exception(
            f"Race condition detected: Is a concurrent `getMore` request for {cc_id} waiting on a response?"
        )
    if not isinstance(last_history_item, CursorResponse):
        return Exception(
            f"Invalid tracking state detected for {cc_id}: Not a valid cursor response."
            "Try issuing your initial query again."
        )
    if last_history_item.ok == 0:  # succeeded: 1, failed: 0.
        return Exception(
            f"`getMore` command failed: Details {last_history_item}. Try again with current continuation?"
        )
    if last_history_item.cursor.partialResultsReturned:
        return Exception(
            "Some database shards queried were unavailable. Try again with current continuation?"
        )
    if last_history_item.cursor.id is None:
        return Exception(
            "All done: Cursor continuation reports zero remaining documents to fetch."
        )
    last_id_retrieved = None
    if isinstance(last_history_item.cursor, InitialCommandResponseCursor):
        last_id_retrieved = last_history_item.cursor.firstBatch[-1]["_id"]
    elif isinstance(last_history_item.cursor, GetMoreCommandResponseCursor):
        last_id_retrieved = last_history_item.cursor.nextBatch[-1]["_id"]
    else:
        return Exception(
            f"Invalid tracking state detected for {cc_id}: Command-response cursor type not recognized. "
            "Try issuing your initial query again."
        )
    initial_command = cc.history[0]
    if isinstance(initial_command, AggregateCommand):
        # TODO construct new `AggregateCommand` from `initial_command`.
        #  Specifically, merge `{"_id": {"$gte": last_id_retrieved}` into first `$match` pipeline stage.
        pass
    elif isinstance(initial_command, FindCommand):
        # TODO construct new `FindCommand` from `initial_command`.
        #  Specifically, merge `{"_id": {"$gte": last_id_retrieved}` into `filter`.
        pass
    else:
        return Exception(
            "Initial command must be `find` or `aggregate` to `getMore` with cursor continuation."
        )
