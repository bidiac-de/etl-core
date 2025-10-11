from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError
from sqlmodel import SQLModel, Session, create_engine, select

import etl_core.persistence.handlers.credentials_handler as H
from etl_core.context.credentials import Credentials
from etl_core.persistence.table_definitions import CredentialsTable


class FakeSecretProvider:
    def __init__(self) -> None:
        self._store: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self._store[key] = value

    def get(self, key: str) -> str:
        if key not in self._store:
            raise KeyError(key)
        return self._store[key]

    def delete(self, key: str) -> None:
        if key in self._store:
            del self._store[key]
        else:
            raise KeyError(key)


def _mk_engine():
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

    @event.listens_for(eng, "connect")
    def _fk_on(dbapi_connection, _):  # noqa: ANN001
        cur = dbapi_connection.cursor()
        cur.execute("PRAGMA foreign_keys=ON")
        cur.close()

    return eng


@pytest.fixture()
def handler(monkeypatch) -> H.CredentialsHandler:
    """
    Use an isolated in-memory engine and a simple in-memory secret provider.
    """
    engine = _mk_engine()

    def _ensure_schema() -> None:
        SQLModel.metadata.create_all(engine)

    monkeypatch.setattr(H, "engine", engine, raising=True)
    monkeypatch.setattr(H, "ensure_schema", _ensure_schema, raising=True)

    sp = FakeSecretProvider()
    monkeypatch.setattr(H, "create_secret_provider", lambda: sp, raising=True)

    return H.CredentialsHandler(engine_=engine)


def _count_rows(sess: Session) -> int:
    return len(sess.exec(select(CredentialsTable)).all())


def test_upsert_create_then_update_and_secret_masking(
    handler: H.CredentialsHandler,
) -> None:
    cid = handler.upsert(
        Credentials(
            name="c1",
            user="u",
            host="h",
            port=1,
            database="db",
            password=None,
            pool_max_size=5,
            pool_timeout_s=10,
        )
    )
    assert isinstance(cid, str)

    cid2 = handler.upsert(
        Credentials(
            name="c1u",
            user="u2",
            host="h2",
            port=2,
            database="db2",
            password="x",
            pool_max_size=7,
            pool_timeout_s=11,
        ),
        credentials_id=cid,
    )
    assert cid2 == cid

    handler.upsert(
        Credentials(
            name="c1uu",
            user="u3",
            host="h3",
            port=3,
            database="db3",
            password="secret",
            pool_max_size=9,
            pool_timeout_s=12,
        ),
        credentials_id=cid,
    )

    got = handler.get_by_id(cid)
    assert got is not None
    creds, returned_id = got
    assert returned_id == cid
    assert creds.user == "u3" and creds.host == "h3"
    assert creds.decrypted_password == "secret"

    with Session(handler.engine) as s:
        assert _count_rows(s) == 1


def test_get_by_id_when_secret_missing(handler: H.CredentialsHandler) -> None:
    cid = handler.upsert(
        Credentials(
            name="c2",
            user="u",
            host="h",
            port=5,
            database="db",
            password=None,
            pool_max_size=1,
            pool_timeout_s=1,
        )
    )
    model = handler.get_by_id(cid)
    assert model is not None
    creds, _ = model
    assert creds.decrypted_password is None


def test_delete_by_id_missing_and_success_and_secret_cleanup(
    handler: H.CredentialsHandler, monkeypatch
) -> None:
    assert handler.delete_by_id(str(uuid4())) is False
    cid = handler.upsert(
        Credentials(
            name="del",
            user="u",
            host="h",
            port=9,
            database="db",
            password="pw",
            pool_max_size=2,
            pool_timeout_s=2,
        )
    )

    def boom(_key: str) -> None:
        raise RuntimeError("backend flake")

    monkeypatch.setattr(handler.secret_store, "delete", boom, raising=True)

    assert handler.delete_by_id(cid) is True

    with Session(handler.engine) as s:
        assert s.get(CredentialsTable, cid) is None


def test_delete_by_id_integrity_error_triggers_rollback(
    handler: H.CredentialsHandler, monkeypatch
) -> None:
    cid = handler.upsert(
        Credentials(
            name="blk",
            user="u",
            host="h",
            port=3,
            database="db",
            password=None,
            pool_max_size=1,
            pool_timeout_s=1,
        )
    )

    orig_delete = Session.delete

    def raise_integrity(self: Session, obj) -> None:  # noqa: ANN001
        if isinstance(obj, CredentialsTable):
            raise IntegrityError("FK", params=None, orig=None)
        return orig_delete(self, obj)

    monkeypatch.setattr(Session, "delete", raise_integrity, raising=True)

    with pytest.raises(IntegrityError):
        handler.delete_by_id(cid)

    with Session(handler.engine) as s:
        assert s.get(CredentialsTable, cid) is not None
