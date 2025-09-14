from __future__ import annotations

from typing import Any, Dict, List

import dask.dataframe as dd
import pandas as pd
import pytest
import json

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.mongodb.mongodb_read import MongoDBRead
from etl_core.components.databases.mongodb.mongodb_write import MongoDBWrite
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.components.databases.pool_args import build_mongo_client_kwargs
from etl_core.components.wiring.schema import Schema
from etl_core.components.wiring.column_definition import FieldDef, DataType

from tests.conftest import (
    seed_docs,
    get_all_docs,
)  # noqa: F401


def _mk_writer(context_id: str, **kwargs) -> MongoDBWrite:
    base = {
        "name": "w",
        "description": "w",
        "comp_type": "write_mongodb",
        "entity_name": "people_ext",
        "context_id": context_id,
    }
    base.update(kwargs)
    return MongoDBWrite(**base)


def _mk_reader(context_id: str, **kwargs) -> MongoDBRead:
    base = {
        "name": "r",
        "description": "r",
        "comp_type": "read_mongodb",
        "entity_name": "people_ext",
        "context_id": context_id,
    }
    base.setdefault(
        "out_port_schemas",
        {"out": Schema(fields=[FieldDef(name="_id", data_type=DataType.INTEGER)])},
    )
    base.update(kwargs)
    return MongoDBRead(**base)


@pytest.mark.asyncio
async def test_read_row_nested_projection_and_sort(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler
    docs = [
        {"_id": 1, "name": "Zoe", "address": {"city": "Berlin", "zip": "10115"}},
        {"_id": 2, "name": "Ada", "address": {"city": "Berlin", "zip": "10117"}},
        {"_id": 3, "name": "Max", "address": {"city": "Hamburg", "zip": "20095"}},
    ]
    await seed_docs(handler, dbname, "people_ext", docs)

    reader = _mk_reader(
        persisted_mongo_context_id,
        query_filter={"address.city": "Berlin"},
        sort=[("name", 1)],
        limit=2,
        skip=0,
        row_batch_size=10,
        out_port_schemas={
            "out": Schema(
                fields=[
                    FieldDef(name="_id", data_type=DataType.INTEGER),
                    FieldDef(name="name", data_type=DataType.STRING),
                    FieldDef(
                        name="address",
                        data_type=DataType.OBJECT,
                        children=[FieldDef(name="city", data_type=DataType.STRING)],
                    ),
                ]
            )
        },
    )

    out_rows: List[Dict[str, Any]] = []
    async for env in reader.process_row(None, metrics):
        out_rows.append(env.payload)

    assert [r["name"] for r in out_rows] == ["Ada", "Zoe"]
    assert len(out_rows) == 2
    for r in out_rows:
        assert "address" in r and isinstance(r["address"], dict)
        assert r["address"].get("city") == "Berlin"
    assert metrics.lines_forwarded == 2


@pytest.mark.asyncio
async def test_read_bulk_nested_with_limit_skip(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler
    docs = [
        {
            "_id": i,
            "grp": "A" if i < 5 else "B",
            "address": {"city": "Berlin" if i % 2 == 0 else "Munich"},
        }
        for i in range(10)
    ]
    await seed_docs(handler, dbname, "people_ext", docs)

    reader = _mk_reader(
        persisted_mongo_context_id,
        query_filter={"grp": "A"},
        sort=[("_id", 1)],
        limit=3,
        skip=1,
        bulk_chunk_size=2,
        out_port_schemas={
            "out": Schema(
                fields=[
                    FieldDef(name="_id", data_type=DataType.INTEGER),
                    FieldDef(
                        name="address",
                        data_type=DataType.OBJECT,
                        children=[FieldDef(name="city", data_type=DataType.STRING)],
                    ),
                ]
            )
        },
    )

    frames: List[pd.DataFrame] = []
    async for env in reader.process_bulk(None, metrics):
        frames.append(env.payload)

    total = sum(len(f) for f in frames)
    assert total == 3
    assert metrics.lines_forwarded == 3

    pdf = pd.concat(frames, ignore_index=True)
    assert "address.city" in pdf.columns
    assert set(pdf["address.city"].unique()) <= {"Berlin", "Munich"}


@pytest.mark.asyncio
async def test_read_bigdata_filter_on_nested(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler
    docs = [
        {
            "_id": i,
            "grp": "G",
            "address": {"city": "Berlin" if i % 3 == 0 else "Hamburg"},
        }
        for i in range(30)
    ]
    await seed_docs(handler, dbname, "people_ext", docs)

    reader = _mk_reader(
        persisted_mongo_context_id,
        query_filter={"address.city": "Berlin"},
        bigdata_partition_chunk_size=7,
    )

    dd_frames: List[dd.DataFrame] = []
    async for env in reader.process_bigdata(None, metrics):
        dd_frames.append(env.payload)

    assert dd_frames
    total_rows = sum(part.compute().shape[0] for part in dd_frames)
    assert total_rows == 10
    assert metrics.lines_forwarded == 10


@pytest.mark.asyncio
async def test_write_row_nested_insert_then_match_filter_update(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler

    writer = _mk_writer(persisted_mongo_context_id, operation=DatabaseOperation.INSERT)
    collected = []
    async for env in writer.process_row(
        {"_id": 100, "name": "Neo", "profile": {"lvl": 1, "skills": ["python", "db"]}},
        metrics,
    ):
        collected.append(env)
    assert collected

    seeded = await get_all_docs(handler, dbname, "people_ext", flt={"_id": 100})
    assert seeded[0]["profile"]["lvl"] == 1

    metrics.lines_forwarded = 0
    metrics.lines_received = 0

    updater = _mk_writer(
        persisted_mongo_context_id,
        operation=DatabaseOperation.UPDATE,
        match_filter={"_id": 100},
        update_fields={"profile"},
    )
    updated = []
    async for env in updater.process_row(
        {"_id": 100, "profile": {"lvl": 2, "skills": ["python"]}}, metrics
    ):
        updated.append(env)
    assert updated

    changed = await get_all_docs(handler, dbname, "people_ext", flt={"_id": 100})
    assert changed[0]["profile"]["lvl"] == 2


@pytest.mark.asyncio
async def test_write_bulk_upsert_composite_keys_and_subset_update(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler

    await seed_docs(
        handler,
        dbname,
        "people_ext",
        [
            {"_id": 201, "tenant": "T1", "name": "A", "age": 30},
            {"_id": 202, "tenant": "T1", "name": "B", "age": 31},
        ],
    )

    writer = _mk_writer(
        persisted_mongo_context_id,
        operation=DatabaseOperation.UPSERT,
        key_fields=["tenant", "_id"],
        update_fields={"name"},
        ordered=True,
    )

    pdf = pd.DataFrame(
        [
            {"_id": 201, "tenant": "T1", "name": "A+"},
            {"_id": 202, "tenant": "T1", "name": "B+"},
        ]
    )
    results = []
    async for env in writer.process_bulk(pdf, metrics):
        results.append(env)
    assert results

    rows = await get_all_docs(handler, dbname, "people_ext", flt={"tenant": "T1"})
    lookup = {r["_id"]: r for r in rows}
    assert lookup[201]["name"] == "A+"


@pytest.mark.asyncio
async def test_write_bigdata_insert_nested_column(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    handler, dbname = mongo_handler

    base = pd.DataFrame(
        {
            "_id": list(range(60)),
            "name": [f"user{i}" for i in range(60)],
            "attrs": [
                {"city": "Berlin"} if i % 2 == 0 else {"city": "Hamburg"}
                for i in range(60)
            ],
        }
    )
    ddf = dd.from_pandas(base, npartitions=6)

    writer = _mk_writer(persisted_mongo_context_id, operation=DatabaseOperation.INSERT)

    emissions = []
    async for env in writer.process_bigdata(ddf=ddf, metrics=metrics):
        emissions.append(env)

    assert len(emissions) == 1
    assert isinstance(emissions[0].payload, dd.DataFrame)
    assert emissions[0].payload.compute().shape[0] == 60

    all_rows = await get_all_docs(handler, dbname, "people_ext")
    assert len(all_rows) == 60

    def _extract_city(attrs_val):
        if isinstance(attrs_val, dict):
            return attrs_val.get("city")
        if isinstance(attrs_val, str):
            try:
                obj = json.loads(attrs_val)
                if isinstance(obj, dict):
                    return obj.get("city")
            except Exception:
                return None
        return None

    cities = {_extract_city(r.get("attrs")) for r in all_rows[:10]}
    cities.discard(None)
    assert cities <= {"Berlin", "Hamburg"}


@pytest.mark.asyncio
async def test_write_then_read_roundtrip_with_auth_db_name(
    persisted_mongo_context_id: str, mongo_handler, metrics
):
    _, dbname = mongo_handler

    writer = MongoDBWrite(
        name="w_auth",
        description="",
        comp_type="write_mongodb",
        entity_name="auth_people",
        context_id=persisted_mongo_context_id,
        operation=DatabaseOperation.INSERT,
        auth_db_name="admin",
    )
    wrote = []
    async for env in writer.process_row({"_id": 777, "name": "Authy"}, metrics):
        wrote.append(env)
    assert wrote

    from etl_core.persistence.handlers.context_handler import ContextHandler

    ctx_handler = ContextHandler()
    loaded = ctx_handler.get_by_id(persisted_mongo_context_id)
    ctx, _ = loaded
    creds, _ = ctx.resolve_active_credentials()

    uri_admin = MongoConnectionHandler.build_uri(
        user=creds.get_parameter("user"),
        password=creds.decrypted_password,
        host=creds.get_parameter("host"),
        port=creds.get_parameter("port"),
        auth_db="admin",
        params=None,
    )
    client_kwargs = build_mongo_client_kwargs(creds)
    h_admin = MongoConnectionHandler()
    h_admin.connect(uri=uri_admin, client_kwargs=client_kwargs)

    rows = await get_all_docs(h_admin, dbname, "auth_people", flt={"_id": 777})
    assert len(rows) == 1

    metrics.lines_forwarded = 0
    metrics.lines_received = 0

    reader = MongoDBRead(
        name="r_auth",
        description="",
        comp_type="read_mongodb",
        entity_name="auth_people",
        context_id=persisted_mongo_context_id,
        auth_db_name="admin",
        query_filter={"_id": 777},
        out_port_schemas={
            "out": Schema(
                fields=[
                    FieldDef(name="_id", data_type=DataType.INTEGER),
                    FieldDef(name="name", data_type=DataType.STRING),
                ]
            )
        },
    )
    out = []
    async for env in reader.process_row(None, metrics):
        out.append(env.payload)
    assert out and out[0]["name"] == "Authy"

    h_admin.close_pool(force=True)
