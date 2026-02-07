from __future__ import annotations

from typing import Dict, Iterable
from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Session, create_engine, select

import etl_core.persistence.handlers.context_handler as ch
import etl_core.persistence.handlers.base_handler as bh
from etl_core.persistence.table_definitions import (
    ContextCredentialsMapTable,
    ContextParameterTable,
    ContextTable,
    CredentialsTable,
)
from etl_core.context.credentials_mapping_context import CredentialsMappingContext


def _mk_engine():
    """SQLite in-memory engine with FK enforcement."""
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, _):  # noqa: ANN001
        cur = dbapi_connection.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    return engine


@pytest.fixture()
def handler(monkeypatch) -> ch.ContextHandler:
    """
    Provide a ContextHandler wired to an isolated in-memory engine.
    We monkeypatch the module-level engine and ensure_schema() the handler uses.
    """
    engine = _mk_engine()

    def _ensure_schema() -> None:
        SQLModel.metadata.create_all(engine)

    monkeypatch.setattr(bh, "engine", engine, raising=True)
    monkeypatch.setattr(bh, "ensure_schema", _ensure_schema, raising=True)

    return ch.ContextHandler()


def _count_params(sess: Session, ctx_id: str) -> Dict[str, int]:
    rows = sess.exec(
        select(ContextParameterTable).where(ContextParameterTable.context_id == ctx_id)
    ).all()
    total = len(rows)
    secure = sum(1 for r in rows if r.is_secure)
    non_secure = total - secure
    return {"total": total, "secure": secure, "non_secure": non_secure}


def _ensure_credentials(sess: Session, ids: Iterable[str]) -> None:
    """
    Insert minimal credential rows for the given ids to satisfy FK from
    ContextCredentialsMapTable.credentials_id.
    """
    lookup = list(ids)
    if not lookup:
        return
    existing = {
        r.id
        for r in sess.exec(
            select(CredentialsTable).where(CredentialsTable.id.in_(lookup))
        ).all()
    }
    for cid in lookup:
        if cid in existing:
            continue
        sess.add(
            CredentialsTable(
                id=cid,
                name=f"cred-{cid}",
                user="u",
                host="h",
                port=5432,
                database="db",
                password=None,
            )
        )
    sess.commit()


def test_upsert_insert_and_replace_parameters(handler: ch.ContextHandler) -> None:
    ctx_id = str(uuid4())

    row = handler.upsert(
        context_id=ctx_id,
        name="ctx",
        environment="DEV",
        non_secure_params={"base": "/tmp", "timeout": "5"},
        secure_param_keys=("password", "token"),
    )
    assert isinstance(row, ContextTable)
    assert row.id == ctx_id and row.name == "ctx" and row.environment == "DEV"

    with handler._session() as s:
        counts = _count_params(s, ctx_id)
        assert counts == {"total": 4, "secure": 2, "non_secure": 2}

    row2 = handler.upsert(
        context_id=ctx_id,
        name="ctx-new",
        environment="TEST",
        non_secure_params={"only": "one"},
        secure_param_keys=("pw",),
    )
    assert row2.name == "ctx-new" and row2.environment == "TEST"

    with handler._session() as s:
        counts2 = _count_params(s, ctx_id)
        assert counts2 == {"total": 2, "secure": 1, "non_secure": 1}


def test_upsert_mapping_then_update_and_get_map(handler: ch.ContextHandler) -> None:
    ctx_id = str(uuid4())

    handler.upsert(
        context_id=ctx_id,
        name="mapctx",
        environment="DEV",
        non_secure_params={},
        secure_param_keys=(),
    )

    with handler._session() as s:
        _ensure_credentials(s, ids=("cred-dev", "cred-test"))

    handler.upsert_credentials_mapping_context(
        context_id=ctx_id,
        name="mapctx",
        environment="DEV",
        mapping_env_to_credentials_id={"DEV": "cred-dev", "TEST": "cred-test"},
    )

    mp = handler.get_credentials_map(ctx_id)
    assert mp == {"DEV": "cred-dev", "TEST": "cred-test"}

    with handler._session() as s:
        _ensure_credentials(s, ids=("cred-dev-2",))

    handler.upsert_credentials_mapping_context(
        context_id=ctx_id,
        name="mapctx2",
        environment="TEST",
        mapping_env_to_credentials_id={"PROD": "cred-dev-2"},
    )
    mp2 = handler.get_credentials_map(ctx_id)
    assert mp2 == {"PROD": "cred-dev-2"}

    all_rows = handler.list_all()
    ids = {r.id for r in all_rows}
    assert ctx_id in ids


def test_get_by_id_found_and_none(handler: ch.ContextHandler) -> None:
    existing_id = str(uuid4())

    handler.upsert(
        context_id=existing_id,
        name="ctx-x",
        environment="PROD",
        non_secure_params={},
        secure_param_keys=(),
    )

    with handler._session() as s:
        _ensure_credentials(s, ids=("cred-prod",))

    handler.upsert_credentials_mapping_context(
        context_id=existing_id,
        name="ctx-x",
        environment="PROD",
        mapping_env_to_credentials_id={"PROD": "cred-prod"},
    )

    got = handler.get_by_id(existing_id)
    assert got is not None
    ctx, returned_id = got
    assert returned_id == existing_id
    assert isinstance(ctx, CredentialsMappingContext)
    assert ctx.name == "ctx-x"
    assert ctx.environment.value == "PROD"
    assert ctx.credentials_ids == {"PROD": "cred-prod"}

    assert handler.get_by_id(str(uuid4())) is None


def test_delete_by_id_missing_and_success(handler: ch.ContextHandler) -> None:
    missing_id = str(uuid4())
    assert handler.delete_by_id(missing_id) is False

    ctx_id = str(uuid4())
    handler.upsert(
        context_id=ctx_id,
        name="to-del",
        environment="DEV",
        non_secure_params={"a": "1"},
        secure_param_keys=("s1",),
    )

    with handler._session() as s:
        _ensure_credentials(s, ids=("c-1",))

    handler.upsert_credentials_mapping_context(
        context_id=ctx_id,
        name="to-del",
        environment="DEV",
        mapping_env_to_credentials_id={"DEV": "c-1"},
    )

    assert handler.delete_by_id(ctx_id) is True

    with handler._session() as s:
        assert (
            s.exec(select(ContextTable).where(ContextTable.id == ctx_id)).first()
            is None
        )
        assert not s.exec(
            select(ContextParameterTable).where(
                ContextParameterTable.context_id == ctx_id
            )
        ).all()
        assert not s.exec(
            select(ContextCredentialsMapTable).where(
                ContextCredentialsMapTable.context_id == ctx_id
            )
        ).all()


def test_delete_by_id_integrity_error_triggers_rollback(
    handler: ch.ContextHandler,
    monkeypatch,
) -> None:
    ctx_id = str(uuid4())
    handler.upsert(
        context_id=ctx_id,
        name="boom",
        environment="DEV",
        non_secure_params={},
        secure_param_keys=(),
    )

    orig_delete = Session.delete

    def boom_delete(self: Session, obj) -> None:  # noqa: ANN001
        if isinstance(obj, ContextTable):
            raise IntegrityError("FK", params=None, orig=None)
        return orig_delete(self, obj)

    monkeypatch.setattr(Session, "delete", boom_delete, raising=True)

    with pytest.raises(IntegrityError):
        handler.delete_by_id(ctx_id)

    with handler._session() as s:
        row = s.exec(select(ContextTable).where(ContextTable.id == ctx_id)).first()
        assert row is not None
