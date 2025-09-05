from __future__ import annotations

from typing import Any, AsyncIterator, Dict, Iterable, List, Optional, Tuple, Sequence

import dask.dataframe as dd
import pandas as pd
from bson import ObjectId
from pymongo.errors import PyMongoError
from pymongo.operations import UpdateOne

from etl_core.components.databases.if_exists_strategy import DatabaseOperation
from etl_core.components.databases.mongodb.mongodb_connection_handler import (
    MongoConnectionHandler,
)
from etl_core.utils.common_helpers import pandas_flatten_docs, unflatten_many


def _to_serializable(doc: Dict[str, Any]) -> Dict[str, Any]:
    # Convert ObjectId to string for downstream portability
    out: Dict[str, Any] = {}
    for k, v in doc.items():
        out[k] = str(v) if isinstance(v, ObjectId) else v
    return out


def _pick_update_fields(
    row: Dict[str, Any], fields: Optional[Iterable[str]]
) -> Dict[str, Any]:
    if not fields:
        return row
    return {k: row[k] for k in fields if k in row}


def _build_match_filter(
    row: Dict[str, Any],
    key_fields: Sequence[str],
    match_filter: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    # Explicit match_filter wins, otherwise derive from key_fields present in the row
    if match_filter:
        return dict(match_filter)
    if key_fields:
        return {k: row.get(k) for k in key_fields if k in row}
    return {}


def _build_update_ops(
    rows: Sequence[Dict[str, Any]],
    *,
    key_fields: Sequence[str],
    match_filter: Optional[Dict[str, Any]],
    update_fields: Optional[Iterable[str]],
    upsert: bool,
) -> List[UpdateOne]:
    ops: List[UpdateOne] = []
    for row in rows:
        flt = _build_match_filter(row, key_fields, match_filter)
        if not flt and not upsert:
            # UPDATE without any filter is not allowed
            raise ValueError("UPDATE requires match_filter or key_fields")
        doc = {"$set": _pick_update_fields(row, update_fields)}
        ops.append(UpdateOne(flt, doc, upsert=upsert))
    return ops


def _build_cursor(
    coll: Any,
    *,
    query_filter: Dict[str, Any],
    projection: Optional[Dict[str, int]],
    sort: Optional[List[Tuple[str, int]]],
    skip: int,
    limit: Optional[int],
    batch_size: int,
) -> Any:
    # Build an AsyncIOMotorCursor for async iteration
    cursor = coll.find(
        filter=query_filter or {},
        projection=projection,
        no_cursor_timeout=False,
    )
    if sort:
        cursor = cursor.sort([(f, 1 if d >= 0 else -1) for f, d in sort])
    if skip:
        cursor = cursor.skip(int(skip))
    if limit is not None:
        cursor = cursor.limit(int(limit))
    return cursor.batch_size(int(batch_size))


async def _apply_bulk_ops(
    coll: Any,
    *,
    operation: DatabaseOperation,
    flat_docs: Sequence[Dict[str, Any]],
    ordered: bool,
    seperator: str,
    key_fields: Sequence[str],
    match_filter: Optional[Dict[str, Any]],
    update_fields: Optional[Iterable[str]],
) -> None:
    """
    Execute the correct MongoDB bulk operation for a list of *flattened* documents.

    Inserts/TRUNCATE: re-nest (unflatten) first so MongoDB stores true nested docs.
    UPDATE/UPSERT: keep flattened key paths for $set, which MongoDB understands.
    """
    if not flat_docs:
        return

    if operation in (DatabaseOperation.TRUNCATE, DatabaseOperation.INSERT):
        nested = unflatten_many(flat_docs, sep=seperator)
        if not nested:
            return
        if operation == DatabaseOperation.TRUNCATE:
            await coll.delete_many({})
        await coll.insert_many(nested, ordered=ordered)
        return

    if operation in (DatabaseOperation.UPDATE, DatabaseOperation.UPSERT):
        upsert = operation == DatabaseOperation.UPSERT
        ops = _build_update_ops(
            flat_docs,
            key_fields=key_fields,
            match_filter=match_filter,
            update_fields=update_fields,
            upsert=upsert,
        )
        if ops:
            await coll.bulk_write(ops, ordered=ordered)
        return

    raise ValueError(f"Unsupported operation: {operation}")


class MongoDBReceiver:
    async def read_row(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        metrics: Any,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, int]],
        sort: Optional[List[Tuple[str, int]]],
        limit: Optional[int],
        skip: int,
        batch_size: int,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream documents one-by-one using a Motor cursor."""
        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                cursor = _build_cursor(
                    coll,
                    query_filter=query_filter,
                    projection=projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    batch_size=batch_size,
                )
                async for doc in cursor:
                    metrics.lines_forwarded += 1
                    yield _to_serializable(doc)
        except PyMongoError as exc:
            raise RuntimeError(f"Mongo read_row failed: {exc}") from exc

    async def read_bulk(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        metrics: Any,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, int]],
        sort: Optional[List[Tuple[str, int]]],
        limit: Optional[int],
        skip: int,
        chunk_size: int,
        seperator: str,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Stream pandas DataFrames in chunks of size `chunk_size` (flattened columns).
        """
        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                cursor = _build_cursor(
                    coll,
                    query_filter=query_filter,
                    projection=projection,
                    sort=sort,
                    skip=skip,
                    limit=limit,
                    batch_size=chunk_size,
                )

                remaining = limit
                buf: List[Dict[str, Any]] = []

                async for doc in cursor:
                    buf.append(_to_serializable(doc))
                    should_flush = len(buf) >= chunk_size or (
                        remaining is not None and len(buf) >= remaining
                    )
                    if should_flush:
                        metrics.lines_forwarded += len(buf)
                        yield pandas_flatten_docs(buf, sep=seperator)
                        if remaining is not None:
                            remaining -= len(buf)
                            if remaining <= 0:
                                return
                        buf.clear()

                if buf:
                    metrics.lines_forwarded += len(buf)
                    yield pandas_flatten_docs(buf, sep=seperator)
        except PyMongoError as exc:
            raise RuntimeError(f"Mongo read_bulk failed: {exc}") from exc

    async def read_bigdata(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        metrics: Any,
        query_filter: Dict[str, Any],
        projection: Optional[Dict[str, int]],
        sort: Optional[List[Tuple[str, int]]],
        limit: Optional[int],
        skip: int,
        chunk_size: int,
        seperator: str,
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Stream Dask DataFrames; each chunk becomes a single-partition DDF.
        Metrics are updated in read_bulk.
        """
        async for pdf in self.read_bulk(
            connection_handler=connection_handler,
            database_name=database_name,
            entity_name=entity_name,
            metrics=metrics,
            query_filter=query_filter,
            projection=projection,
            sort=sort,
            limit=limit,
            skip=skip,
            chunk_size=chunk_size,
            seperator=seperator,
        ):
            yield dd.from_pandas(pdf, npartitions=1)

    async def write_row(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        row: Dict[str, Any],
        metrics: Any,
        operation: DatabaseOperation,
        write_options: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Write one document, update metrics accordingly."""
        key_fields: List[str] = write_options["key_fields"]
        update_fields = write_options["update_fields"]
        match_filter = write_options["match_filter"]

        metrics.lines_received += 1

        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                if operation == DatabaseOperation.TRUNCATE:
                    await coll.delete_many({})
                    res = await coll.insert_one(row)
                    metrics.lines_forwarded += 1
                    return {"inserted_id": str(res.inserted_id), "row": row}

                if operation == DatabaseOperation.INSERT:
                    res = await coll.insert_one(row)
                    metrics.lines_forwarded += 1
                    return {"inserted_id": str(res.inserted_id), "row": row}

                flt = dict(match_filter or {})
                if not flt and key_fields:
                    flt = {k: row.get(k) for k in key_fields if k in row}

                if operation == DatabaseOperation.UPSERT:
                    doc = {"$set": _pick_update_fields(row, update_fields)}
                    res = await coll.update_one(filter=flt, update=doc, upsert=True)
                    metrics.lines_forwarded += 1
                    return {
                        "matched": res.matched_count,
                        "modified": res.modified_count,
                        "upserted_id": (
                            str(res.upserted_id) if res.upserted_id else None
                        ),
                        "row": row,
                    }

                if operation == DatabaseOperation.UPDATE:
                    if not flt:
                        raise ValueError("UPDATE requires match_filter or key_fields")
                    doc = {"$set": _pick_update_fields(row, update_fields)}
                    res = await coll.update_many(filter=flt, update=doc, upsert=False)
                    metrics.lines_forwarded += 1
                    return {
                        "matched": res.matched_count,
                        "modified": res.modified_count,
                        "row": row,
                    }

                raise ValueError(f"Unsupported operation: {operation}")
        except PyMongoError as exc:
            raise RuntimeError(f"Mongo write_row failed: {exc}") from exc

    async def write_bulk(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        frame: pd.DataFrame,
        metrics: Any,
        operation: DatabaseOperation,
        write_options: Dict[str, Any],
        seperator: str,
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Write a pandas DataFrame of *flattened* columns.
        - INSERT/TRUNCATE: re-nest records with `unflatten_many` before insert.
        - UPDATE/UPSERT: keep flattened keys for $set paths.
        """
        ordered: bool = bool(write_options.get("ordered", True))
        key_fields: List[str] = list(write_options.get("key_fields", []))
        update_fields = write_options.get("update_fields")
        match_filter = write_options.get("match_filter")

        n_rows = int(frame.shape[0])
        metrics.lines_received += n_rows
        flat_docs: List[Dict[str, Any]] = frame.to_dict(orient="records")

        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                await _apply_bulk_ops(
                    coll,
                    operation=operation,
                    flat_docs=flat_docs,
                    ordered=ordered,
                    seperator=seperator,
                    key_fields=key_fields,
                    match_filter=match_filter,
                    update_fields=update_fields,
                )
                metrics.lines_forwarded += n_rows
                yield frame
        except PyMongoError as exc:
            raise RuntimeError(f"Mongo write_bulk failed: {exc}") from exc

    async def write_bigdata(
        self,
        *,
        connection_handler: MongoConnectionHandler,
        database_name: str,
        entity_name: str,
        frame: dd.DataFrame,
        metrics: Any,
        operation: DatabaseOperation,
        write_options: Dict[str, Any],
        seperator: str,
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Bigdata write, processing dask DataFrame partitions one-by-one.
        """
        ordered: bool = bool(write_options.get("ordered", True))
        key_fields: List[str] = list(write_options.get("key_fields", []))
        update_fields = write_options.get("update_fields")
        match_filter = write_options.get("match_filter")

        nparts = int(frame.npartitions)
        frame = frame.repartition(npartitions=nparts)

        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                for i in range(nparts):
                    part_df = frame.get_partition(i).compute()
                    n_rows = int(part_df.shape[0])
                    if n_rows == 0:
                        continue

                    metrics.lines_received += n_rows
                    flat_docs: List[Dict[str, Any]] = part_df.to_dict(orient="records")

                    await _apply_bulk_ops(
                        coll,
                        operation=operation,
                        flat_docs=flat_docs,
                        ordered=ordered,
                        seperator=seperator,
                        key_fields=key_fields,
                        match_filter=match_filter,
                        update_fields=update_fields,
                    )
                    metrics.lines_forwarded += n_rows

                # final yield, all partitions processed
                yield frame
        except PyMongoError as exc:
            raise RuntimeError(f"Mongo write_bigdata failed: {exc}") from exc
