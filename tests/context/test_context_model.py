from __future__ import annotations

from itertools import count
from types import SimpleNamespace
from typing import Dict, Optional, Tuple

import pytest

from etl_core.context.context import Context
from etl_core.context.context_parameter import ContextParameter
from etl_core.context.environment import Environment
from etl_core.context.credentials import Credentials


_id_counter = count(1)


def _param(key: str, value: str) -> ContextParameter:
    return ContextParameter(
        id=next(_id_counter),
        key=key,
        value=value,
        type="STRING",
        is_secure=False,
    )


def _ctx(**overrides) -> Context:
    base = dict(name="c1", environment=Environment.DEV, parameters={})
    base.update(overrides)
    return Context(**base)


class _Repo:
    def __init__(self, mapping: Dict[str, Credentials]) -> None:
        self.mapping = mapping
        self.calls = 0

    def get_by_id(self, cred_id: str) -> Optional[Tuple[Credentials, str]]:
        self.calls += 1
        v = self.mapping.get(cred_id)
        if v is None:
            return None
        return v, "provider-1"


def test_normalize_parameters_accepts_dict_and_list() -> None:
    p1 = _param("A", "1")
    p2 = _param("B", "2")

    c_list = _ctx(parameters=[p1, p2])
    assert set(c_list.parameters.keys()) == {"A", "B"}
    assert c_list.get_parameter("A") == "1"
    assert c_list.get_parameter("B") == "2"

    c_dict = _ctx(parameters={"A": p1, "B": p2})
    assert set(c_dict.parameters.keys()) == {"A", "B"}
    assert c_dict.get_parameter("A") == "1"
    assert c_dict.get_parameter("B") == "2"


def test_normalize_parameters_invalid_type_raises() -> None:
    with pytest.raises(TypeError):
        _ctx(parameters=42)  # type: ignore[arg-type]


def test_get_and_set_parameter_ok_and_missing() -> None:
    p = _param("X", "old")
    c = _ctx(parameters={"X": p})
    assert c.get_parameter("X") == "old"

    c.set_parameter("X", "new")
    assert c.get_parameter("X") == "new"

    with pytest.raises(KeyError):
        c.set_parameter("MISSING", "v")


def test_add_and_get_credentials_from_cache_without_repo() -> None:
    c = _ctx()
    token = SimpleNamespace(secret="s1")
    c.add_credentials("id1", token)
    got = c.get_credentials("id1")
    assert got is token


def test_get_credentials_without_repo_raises_keyerror() -> None:
    c = _ctx()
    with pytest.raises(KeyError):
        c.get_credentials("id2")


def test_get_credentials_repo_returns_none_raises_keyerror() -> None:
    c = _ctx()
    repo = _Repo(mapping={})
    c.attach_credentials_repository(repo)
    with pytest.raises(KeyError):
        c.get_credentials("id3")
    assert repo.calls == 1


def test_get_credentials_loaded_then_cached() -> None:
    c = _ctx()
    token = SimpleNamespace(secret="s2")
    repo = _Repo(mapping={"id4": token})
    c.attach_credentials_repository(repo)

    loaded = c.get_credentials("id4")
    assert loaded is token
    assert repo.calls == 1

    loaded_again = c.get_credentials("id4")
    assert loaded_again is token
    assert repo.calls == 1
