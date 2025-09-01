from __future__ import annotations

from typing import Any, AsyncIterator, Dict, List, Sequence

import dask.dataframe as dd
import pandas as pd
import pytest
from bson import ObjectId

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.receivers.databases.mongodb.mongodb_receiver import MongoDBReceiver


@pytest.fixture
def receiver() -> MongoDBReceiver:
    return MongoDBReceiver()


async def _seed(
    handler,
    database: str,
    collection: str,
    docs: Sequence[Dict[str, Any]],
) -> None:
    if not docs:
        return
    with handler.lease_collection(database=database, collection=collection) as (
        _,
        coll,
    ):
        await coll.insert_many(list(docs), ordered=True)


async def _collect_aiter(ait: AsyncIterator[Any]) -> List[Any]:
    out: List[Any] = []
    async for x in ait:
        out.append(x)
    return out


@pytest.mark.asyncio
async def test_read_row_streaming_and_objectid_serialization(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "people"

    oid = ObjectId()
    await _seed(
        handler,
        db,
        coll,
        [
            {"_id": oid, "a": 1, "nested": {"x": "y"}},
            {"_id": ObjectId(), "a": 2, "nested": {"x": "z"}},
        ],
    )

    rows: List[Dict[str, Any]] = []
    async for row in receiver.read_row(
        connection_handler=handler,
        database_name=db,
        entity_name=coll,
        metrics=metrics,
        query_filter={},
        projection=None,
        sort=[("a", 1)],
        limit=None,
        skip=0,
        batch_size=2,
    ):
        rows.append(row)

    assert [r["a"] for r in rows] == [1, 2]
    # Receiver should stringify ObjectId for JSON-serializable output
    assert rows[0]["_id"] == str(oid)
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_read_bulk_projection_limit_skip(
    receiver: MongoDBReceiver,
    metrics,  # ComponentMetrics
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "items"
    await _seed(
        handler,
        db,
        coll,
        [{"_id": i, "a": i, "b": {"c": i * 10}} for i in range(7)],
    )

    chunks: List[pd.DataFrame] = []
    async for frame in receiver.read_bulk(
        connection_handler=handler,
        database_name=db,
        entity_name=coll,
        metrics=metrics,
        query_filter={},
        projection={"_id": 0, "a": 1, "b.c": 1},
        sort=None,
        limit=5,
        skip=0,
        chunk_size=3,
        seperator=".",
    ):
        chunks.append(frame)

    assert [len(c) for c in chunks] == [3, 2]
    # flattened columns (as produced by pandas_flatten_docs in receiver)
    assert list(chunks[0].columns) == ["a", "b.c"]
    assert metrics.lines_forwarded == 5


@pytest.mark.asyncio
async def test_read_bigdata_emits_partitions(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "logs"
    await _seed(handler, db, coll, [{"_id": i, "a": i} for i in range(8)])

    ddfs: List[dd.DataFrame] = []
    async for ddf in receiver.read_bigdata(
        connection_handler=handler,
        database_name=db,
        entity_name=coll,
        metrics=metrics,
        query_filter={},
        projection={"_id": 0, "a": 1},
        sort=None,
        limit=None,
        skip=0,
        chunk_size=3,
        seperator=".",
    ):
        ddfs.append(ddf)

    # Should emit multiple partitions wrapping the bulk frames
    assert len(ddfs) >= 3
    total = sum(int(ddf.shape[0].compute()) for ddf in ddfs)
    assert total == 8
    assert metrics.lines_forwarded == 8


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "op, expect_keys_subset",
    [
        (DatabaseOperation.INSERT, {"inserted_id", "row"}),
        (DatabaseOperation.TRUNCATE, {"inserted_id", "row"}),
        (DatabaseOperation.UPSERT, {"row"}),
    ],
)
async def test_write_row_insert_upsert_truncate(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
    op: DatabaseOperation,
    expect_keys_subset: set,
) -> None:
    handler, db = mongo_handler
    coll = "write_row"

    # Start with one existing row for upsert matching
    await _seed(handler, db, coll, [{"k": 1, "val": "old"}])

    res = await receiver.write_row(
        connection_handler=handler,
        database_name=db,
        entity_name=coll,
        row={"k": 1, "val": "new"},
        metrics=metrics,
        operation=op,
        write_options={
            "key_fields": ["k"],
            "update_fields": ["val"],
            "match_filter": None,
            "ordered": True,
        },
    )

    assert expect_keys_subset.issubset(res.keys())
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1

    # Verify state after write
    with handler.lease_collection(database=db, collection=coll) as (_, c):
        docs = [
            d async for d in c.find(filter={}, projection={"_id": 0, "k": 1, "val": 1})
        ]
    if op is DatabaseOperation.TRUNCATE:
        # After truncate, only the new row should remain
        assert docs == [{"k": 1, "val": "new"}]
    elif op is DatabaseOperation.UPSERT:
        assert {"k": 1, "val": "new"} in docs
    else:
        # INSERT adds another row with same key (no unique index in mongomock)
        assert {"k": 1, "val": "old"} in docs and {"k": 1, "val": "new"} in docs


@pytest.mark.asyncio
async def test_write_bulk_insert_many(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "bulk_insert"

    frame = pd.DataFrame([{"k": 1, "v": "a"}, {"k": 2, "v": "b"}, {"k": 3, "v": "c"}])

    results = await _collect_aiter(
        receiver.write_bulk(
            connection_handler=handler,
            database_name=db,
            entity_name=coll,
            frame=frame,
            metrics=metrics,
            operation=DatabaseOperation.INSERT,
            write_options={
                "key_fields": ["k"],
                "update_fields": ["v"],
                "match_filter": None,
                "ordered": True,
            },
            seperator=".",
        )
    )
    # one result per chunk
    assert results
    assert metrics.lines_received == 3
    assert metrics.lines_forwarded == 3

    with handler.lease_collection(database=db, collection=coll) as (_, c):
        docs = [
            d async for d in c.find(filter={}, projection={"_id": 0, "k": 1, "v": 1})
        ]
    assert sorted(docs, key=lambda d: d["k"]) == [
        {"k": 1, "v": "a"},
        {"k": 2, "v": "b"},
        {"k": 3, "v": "c"},
    ]


@pytest.mark.asyncio
async def test_write_bulk_upsert_builds_update_ops(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "bulk_upsert"

    # Seed with one conflicting key to exercise matched vs upsert
    await _seed(handler, db, coll, [{"k": 1, "v": "old"}])

    frame = pd.DataFrame([{"k": 1, "v": "new"}, {"k": 2, "v": "b"}])

    results = await _collect_aiter(
        receiver.write_bulk(
            connection_handler=handler,
            database_name=db,
            entity_name=coll,
            frame=frame,
            metrics=metrics,
            operation=DatabaseOperation.UPSERT,
            write_options={
                "key_fields": ["k"],
                "update_fields": ["v"],
                "match_filter": None,
                "ordered": True,
            },
            seperator=".",
        )
    )
    assert results  # at least one bulk result emitted
    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2

    with handler.lease_collection(database=db, collection=coll) as (_, c):
        docs = [
            d async for d in c.find(filter={}, projection={"_id": 0, "k": 1, "v": 1})
        ]
    # key=1 updated, key=2 inserted
    assert sorted(docs, key=lambda d: d["k"]) == [
        {"k": 1, "v": "new"},
        {"k": 2, "v": "b"},
    ]


@pytest.mark.asyncio
async def test_write_bulk_truncate_clears_then_inserts(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "bulk_truncate"

    await _seed(handler, db, coll, [{"k": 1, "v": "old"}])

    frame = pd.DataFrame([{"k": 9, "v": "x"}, {"k": 10, "v": "y"}])

    _ = await _collect_aiter(
        receiver.write_bulk(
            connection_handler=handler,
            database_name=db,
            entity_name=coll,
            frame=frame,
            metrics=metrics,
            operation=DatabaseOperation.TRUNCATE,
            write_options={
                "key_fields": ["k"],
                "update_fields": ["v"],
                "match_filter": None,
                "ordered": True,
            },
            seperator=".",
        )
    )

    with handler.lease_collection(database=db, collection=coll) as (_, c):
        docs = [
            d async for d in c.find(filter={}, projection={"_id": 0, "k": 1, "v": 1})
        ]
    assert sorted(docs, key=lambda d: d["k"]) == [
        {"k": 9, "v": "x"},
        {"k": 10, "v": "y"},
    ]
    assert metrics.lines_received == 2
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_write_bigdata_partitions(
    receiver: MongoDBReceiver,
    metrics,
    mongo_handler,
) -> None:
    handler, db = mongo_handler
    coll = "big_write"

    pdf = pd.DataFrame([{"k": i, "v": f"v{i}"} for i in range(7)])
    ddf = dd.from_pandas(pdf, npartitions=3)

    results = await _collect_aiter(
        receiver.write_bigdata(
            connection_handler=handler,
            database_name=db,
            entity_name=coll,
            frame=ddf,
            metrics=metrics,
            operation=DatabaseOperation.INSERT,
            write_options={
                "key_fields": ["k"],
                "update_fields": ["v"],
                "match_filter": None,
                "ordered": True,
            },
            seperator=".",
        )
    )
    # one result per partition
    assert len(results) == 3
    assert metrics.lines_received == 7
    assert metrics.lines_forwarded == 7

    with handler.lease_collection(database=db, collection=coll) as (_, c):
        docs = [
            d async for d in c.find(filter={}, projection={"_id": 0, "k": 1, "v": 1})
        ]
    assert len(docs) == 7
