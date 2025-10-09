from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pytest
from keyring.errors import KeyringError, PasswordDeleteError

from src.etl_core.context.secrets.keyring_provider import KeyringSecretProvider


class TestKeyringSecretProvider:
    """Tests for KeyringSecretProvider using monkeypatched keyring API."""

    def setup_method(self) -> None:
        self.service = "etl-core-tests"
        self.provider = KeyringSecretProvider(self.service)

    def test_set_delegates_to_keyring(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: List[Tuple[str, str, str]] = []

        def fake_set_password(
            service: str, key: str, secret: str
        ) -> None:  # noqa: D401
            calls.append((service, key, secret))

        monkeypatch.setattr("keyring.set_password", fake_set_password)

        self.provider.set("k", "s")
        assert calls == [(self.service, "k", "s")]

    def test_get_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get_password(service: str, key: str) -> str:
            assert service == self.service and key == "k"
            return "secret"

        monkeypatch.setattr("keyring.get_password", fake_get_password)

        assert self.provider.get("k") == "secret"

    def test_get_missing_raises_keyerror(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get_password(service: str, key: str) -> None:  # type: ignore[override]
            return None

        monkeypatch.setattr("keyring.get_password", fake_get_password)

        with pytest.raises(
            KeyError,
            match=r"Secret not found for service='etl-core-tests' key='missing'",
        ):
            self.provider.get("missing")

    def test_exists_true_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        store: Dict[str, str] = {"present": "v"}

        def fake_get_password(service: str, key: str) -> Any:
            assert service == self.service
            return store.get(key)

        monkeypatch.setattr("keyring.get_password", fake_get_password)

        assert self.provider.exists("present") is True
        assert self.provider.exists("absent") is False

    def test_exists_wraps_keyring_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_get_password(service: str, key: str) -> str:
            raise KeyringError("boom")

        monkeypatch.setattr("keyring.get_password", fake_get_password)

        with pytest.raises(RuntimeError, match=r"keyring exists failed: boom"):
            self.provider.exists("k")

    def test_delete_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        calls: List[Tuple[str, str]] = []

        def fake_delete_password(service: str, key: str) -> None:
            calls.append((service, key))

        monkeypatch.setattr("keyring.delete_password", fake_delete_password)

        self.provider.delete("k")
        assert calls == [(self.service, "k")]

    def test_delete_password_delete_error_is_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def fake_delete_password(service: str, key: str) -> None:
            raise PasswordDeleteError("not there")

        monkeypatch.setattr("keyring.delete_password", fake_delete_password)

        self.provider.delete("missing")

    def test_delete_wraps_keyring_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def fake_delete_password(service: str, key: str) -> None:
            raise KeyringError("bad backend")

        monkeypatch.setattr("keyring.delete_password", fake_delete_password)

        with pytest.raises(RuntimeError, match=r"keyring delete failed: bad backend"):
            self.provider.delete("k")
