from __future__ import annotations

import asyncio
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import mongomock
from pymongo.operations import UpdateOne


class _AsyncCursor:
    """Tiny async cursor over a materialized list of docs."""

    def __init__(self, docs: List[Dict[str, Any]]) -> None:
        self._docs = docs
        self._idx = 0
        self._batch_size = 0

    def sort(self, spec: List[Tuple[str, int]] | None) -> "_AsyncCursor":
        if spec:
            for field, direction in reversed(spec):
                reverse = direction < 0
                self._docs.sort(key=lambda d, f=field: d.get(f), reverse=reverse)
        return self

    def skip(self, n: int | None) -> "_AsyncCursor":
        if n:
            self._docs = self._docs[int(n) :]
        return self

    def limit(self, n: Optional[int]) -> "_AsyncCursor":
        if n is not None:
            self._docs = self._docs[: int(n)]
        return self

    def batch_size(self, n: int | None) -> "_AsyncCursor":
        self._batch_size = int(n or 0)
        return self

    def __aiter__(self) -> "_AsyncCursor":
        self._idx = 0
        return self

    async def __anext__(self) -> Dict[str, Any]:
        if self._idx >= len(self._docs):
            raise StopAsyncIteration
        if self._batch_size and self._idx % max(self._batch_size, 1) == 0:
            await asyncio.sleep(0)
        item = self._docs[self._idx]
        self._idx += 1
        return item


class _AsyncCollection:
    """Motor-like async wrapper around mongomock collection."""

    def __init__(self, coll: mongomock.collection.Collection) -> None:
        self._coll = coll

    def find(
        self,
        *,
        filter: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        no_cursor_timeout: bool = False,  # noqa: ARG002
    ) -> _AsyncCursor:
        cursor = list(self._coll.find(filter or {}, projection=projection))
        return _AsyncCursor(cursor)

    async def insert_one(self, doc: Dict[str, Any]):
        res = self._coll.insert_one(doc)
        await asyncio.sleep(0)
        return res

    async def insert_many(
        self, docs: Sequence[Dict[str, Any]], *, ordered: bool = True
    ):  # noqa: ARG002
        res = self._coll.insert_many(list(docs))
        await asyncio.sleep(0)
        return res

    async def delete_many(self, flt: Dict[str, Any]):
        res = self._coll.delete_many(flt or {})
        await asyncio.sleep(0)
        return res

    async def update_one(
        self,
        *,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,
    ):
        res = self._coll.update_one(filter, update, upsert=upsert)
        await asyncio.sleep(0)
        return res

    async def update_many(
        self,
        *,
        filter: Dict[str, Any],
        update: Dict[str, Any],
        upsert: bool = False,  # noqa: ARG002
    ):
        res = self._coll.update_many(filter, update)
        await asyncio.sleep(0)
        return res

    async def bulk_write(
        self, ops: Iterable[UpdateOne], *, ordered: bool = True
    ):  # noqa: ARG002
        """
        Minimal bulk_write for UpdateOne operations to
        avoid mongomock kwargs incompatibilities.
        """
        matched = 0
        modified = 0
        upserted = 0
        for op in ops:
            if not isinstance(op, UpdateOne):
                raise NotImplementedError(f"Unsupported bulk op: {type(op).__name__}")
            res = self._coll.update_one(  # noqa: SLF001
                op._filter, op._doc, upsert=bool(op._upsert)
            )
            matched += res.matched_count
            modified += getattr(res, "modified_count", 0)
            upserted += 1 if getattr(res, "upserted_id", None) is not None else 0

        class _Result:
            matched_count = matched
            modified_count = modified
            upserted_count = upserted

        await asyncio.sleep(0)
        return _Result()


class _AsyncDatabase:
    def __init__(self, db: mongomock.database.Database) -> None:
        self._db = db

    def __getitem__(self, name: str) -> _AsyncCollection:
        return _AsyncCollection(self._db[name])


class AsyncMongoMockClient:
    """
    A tiny, Motor-compatible async client backed by mongomock.
    Only implements the subset your receivers/components use.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:  # noqa: D401, ANN401
        self._client = mongomock.MongoClient(*args, **kwargs)

    def __getitem__(self, name: str) -> _AsyncDatabase:
        return _AsyncDatabase(self._client[name])

    def __getattr__(self, item: str) -> Any:
        """
        Pass through unknown attributes to the underlying mongomock client.
        This enables list_database_names(), get_database(), etc., if needed.
        """
        return getattr(self._client, item)

    def drop_database(self, name: str) -> None:
        """Expose mongomock's drop_database so tests can cleanly remove per-test DBs."""
        self._client.drop_database(name)

    def close(self) -> None:
        """
        Reset the in-memory server. Pool code calls this; giving a fresh
        mongomock instance ensures no cross-test bleed even if pools linger.
        """
        self._client = mongomock.MongoClient()
