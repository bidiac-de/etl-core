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


async def _truncate_and_insert_many(
    coll: Any, docs: Sequence[Dict[str, Any]], *, ordered: bool
) -> None:
    # Clear the collection, then insert the incoming docs (if any)
    await coll.delete_many({})
    if docs:
        await coll.insert_many(list(docs), ordered=ordered)


async def _insert_many(coll: Any, docs: Sequence[Dict[str, Any]], *, ordered: bool) -> None:
    if docs:
        await coll.insert_many(list(docs), ordered=ordered)


def _build_match_filter(
    row: Dict[str, Any], key_fields: Sequence[str], match_filter: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    # Explicit match_filter wins; otherwise derive from key_fields present in the row
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
            # Mirror previous behavior: UPDATE without any filter is not allowed
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
    # Build an AsyncIOMotorCursor with common options
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
        """
        Stream documents one-by-one using a Motor cursor.
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
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Stream pandas DataFrames in chunks of size `chunk_size`.
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
                    if len(buf) >= chunk_size or (
                        remaining is not None and len(buf) >= remaining
                    ):
                        metrics.lines_forwarded += len(buf)
                        yield pd.DataFrame(buf)
                        if remaining is not None:
                            remaining -= len(buf)
                            if remaining <= 0:
                                return
                        buf.clear()

                if buf:
                    metrics.lines_forwarded += len(buf)
                    yield pd.DataFrame(buf)
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
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Stream Dask DataFrames; each chunk becomes a single-partition DDF.
        Metrics are updated in read_bulk; do not double-count here.
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
        """
        Write one document, update metrics accordingly.
        """
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
                        "upserted_id": str(res.upserted_id) if res.upserted_id else None,
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
    ) -> AsyncIterator[pd.DataFrame]:
        """
        Write a DataFrame with Motor bulk ops; yield the same frame once.
        Metrics: lines_received += len(frame), lines_forwarded += len(frame).
        """
        if frame.empty:
            # Nothing to do; still yield for contract consistency
            yield frame
            return

        n_rows = int(len(frame))
        metrics.lines_received += n_rows

        key_fields: List[str] = write_options.get("key_fields", [])
        update_fields = write_options.get("update_fields")
        match_filter = write_options.get("match_filter")
        ordered: bool = bool(write_options.get("ordered", True))

        # Convert once; reuse in all branches
        docs: List[Dict[str, Any]] = frame.to_dict(orient="records")

        try:
            with connection_handler.lease_collection(
                database=database_name, collection=entity_name
            ) as (_, coll):
                if operation == DatabaseOperation.TRUNCATE:
                    await _truncate_and_insert_many(coll, docs, ordered=ordered)

                elif operation == DatabaseOperation.INSERT:
                    await _insert_many(coll, docs, ordered=ordered)

                elif operation in (DatabaseOperation.UPDATE, DatabaseOperation.UPSERT):
                    upsert = operation == DatabaseOperation.UPSERT
                    ops = _build_update_ops(
                        docs,
                        key_fields=key_fields,
                        match_filter=match_filter,
                        update_fields=update_fields,
                        upsert=upsert,
                    )
                    if ops:
                        await coll.bulk_write(ops, ordered=ordered)

                else:
                    raise ValueError(f"Unsupported operation: {operation}")

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
    ) -> AsyncIterator[dd.DataFrame]:
        """
        Write a Dask DataFrame partition-by-partition using bulk ops.
        """
        for i in range(frame.npartitions):
            pdf = frame.get_partition(i).compute()
            if pdf.empty:
                continue
            rows = int(len(pdf))
            metrics.lines_received += rows
            async for result in self.write_bulk(
                connection_handler=connection_handler,
                database_name=database_name,
                entity_name=entity_name,
                frame=pdf,
                metrics=metrics,
                operation=operation,
                write_options=write_options,
            ):
                yield dd.from_pandas(result, npartitions=1)
