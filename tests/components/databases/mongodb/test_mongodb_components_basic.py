from __future__ import annotations

import dask.dataframe as dd
import pytest

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.mongodb.mongodb_read import MongoDBRead
from etl_core.components.databases.mongodb.mongodb_write import MongoDBWrite
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

from tests.conftest import (
    seed_docs,
    get_all_docs,
)  # noqa: F401


@pytest.mark.asyncio
async def test_mongodb_read_row(mongo_context, mongo_handler, sample_docs, metrics):
    handler, dbname = mongo_handler
    await seed_docs(handler, dbname, "people", sample_docs)

    reader = MongoDBRead(
        name="read",
        description="read",
        comp_type="read_mongodb",
        entity_name="people",
        credentials_id=101,
        query_filter={"city": "Berlin"},
        sort=[("name", 1)],
        limit=1,
        skip=0,
        row_batch_size=2,
        out_port_schemas={
            "out": Schema(
                fields=[
                    FieldDef(name="_id", data_type=DataType.INTEGER),
                    FieldDef(name="name", data_type=DataType.STRING),
                ]
            )
        },
    )
    reader.context = mongo_context

    docs = []
    async for env in reader.process_row(None, metrics):
        docs.append(env.payload)

    assert len(docs) == 1
    assert set(docs[0].keys()) <= {"_id", "name"}
    assert metrics.lines_forwarded == 1


@pytest.mark.asyncio
async def test_mongodb_read_bulk_and_bigdata(mongo_context, mongo_handler, metrics):
    handler, dbname = mongo_handler
    seed = [{"_id": i, "k": i, "tag": "x" if i % 2 == 0 else "y"} for i in range(4)]
    await seed_docs(handler, dbname, "events", seed)

    reader = MongoDBRead(
        name="read2",
        description="read2",
        comp_type="read_mongodb",
        entity_name="events",
        credentials_id=101,
        bulk_chunk_size=3,
        query_filter={},
        out_port_schemas={
            "out": Schema(
                fields=[
                    FieldDef(name="_id", data_type=DataType.INTEGER),
                    FieldDef(name="k", data_type=DataType.INTEGER),
                    FieldDef(name="tag", data_type=DataType.STRING),
                ]
            )
        },
    )
    reader.context = mongo_context

    frames = []
    async for env in reader.process_bulk(None, metrics):
        frames.append(env.payload)

    assert sum(len(f) for f in frames) == 4
    # flattened columns expected
    assert all({"_id", "k", "tag"}.issubset(set(f.columns)) for f in frames)
    assert metrics.lines_forwarded == 4

    # reset metrics counters before next phase
    metrics.lines_forwarded = 0
    metrics.lines_received = 0

    reader_big = MongoDBRead(
        name="read3",
        description="read3",
        comp_type="read_mongodb",
        entity_name="events",
        credentials_id=101,
        bigdata_partition_chunk_size=2,
        out_port_schemas={
            "out": Schema(fields=[FieldDef(name="_id", data_type=DataType.INTEGER)])
        },
    )
    reader_big.context = mongo_context
    dd_out = []
    async for env in reader_big.process_bigdata(None, metrics):
        dd_out.append(env.payload)

    assert all(isinstance(x, dd.DataFrame) for x in dd_out)
    assert sum(x.compute().shape[0] for x in dd_out) == 4


def _mk_writer(mongo_context, **kwargs) -> MongoDBWrite:
    base = {
        "name": "w",
        "description": "w",
        "comp_type": "write_mongodb",
        "entity_name": "people",
        "credentials_id": 101,
    }
    base.update(kwargs)
    w = MongoDBWrite(**base)
    w.context = mongo_context
    return w


@pytest.mark.asyncio
async def test_mongodb_write_insert_and_truncate(
    mongo_context, mongo_handler, sample_pdf, metrics
):
    handler, dbname = mongo_handler

    writer = _mk_writer(mongo_context, operation=DatabaseOperation.INSERT)
    outs = []
    async for env in writer.process_bulk(sample_pdf, metrics):
        outs.append(env.payload)

    assert outs and outs[0].shape[0] == sample_pdf.shape[0]
    assert metrics.lines_received == sample_pdf.shape[0]
    assert metrics.lines_forwarded == sample_pdf.shape[0]

    docs = await get_all_docs(handler, dbname, "people")
    assert len(docs) == sample_pdf.shape[0]

    # reset metrics before second phase
    metrics.lines_forwarded = 0
    metrics.lines_received = 0

    row_writer = _mk_writer(mongo_context, operation=DatabaseOperation.TRUNCATE)
    res = []
    async for env in row_writer.process_row({"_id": 999, "name": "Zoe"}, metrics):
        res.append(env.payload)

    assert res[0]["inserted_id"] is not None
    assert metrics.lines_received == 1
    assert metrics.lines_forwarded == 1

    docs2 = await get_all_docs(handler, dbname, "people")
    assert len(docs2) == 1
    assert docs2[0]["_id"] == 999


def test_mongodb_component_ports_and_connection(mongo_context):
    r = MongoDBRead(
        name="r",
        description="r",
        comp_type="read_mongodb",
        entity_name="col",
        credentials_id=101,
        out_port_schemas={
            "out": Schema(fields=[FieldDef(name="_id", data_type=DataType.INTEGER)])
        },
    )
    r.context = mongo_context
    assert len(r.OUTPUT_PORTS) == 1
    assert r.OUTPUT_PORTS[0].name == "out"

    w = MongoDBWrite(
        name="w",
        description="w",
        comp_type="write_mongodb",
        entity_name="col",
        credentials_id=101,
        operation=DatabaseOperation.INSERT,
    )
    w.context = mongo_context
    assert len(w.INPUT_PORTS) == 1
    assert len(w.OUTPUT_PORTS) == 1

    assert hasattr(r, "connection_handler")
    assert hasattr(w, "connection_handler")
