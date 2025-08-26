from __future__ import annotations
from typing import Optional
import keyring
from keyring.errors import KeyringError
from etl_core.context.secrets.secret_provider import SecretProvider


class KeyringSecretProvider(SecretProvider):
    """
    OS keychain-backed secret provider using keyring package.
    Store secrets outside the DB/config files.
    """

    def __init__(self, service: str) -> None:
        self._service = service

    def get(self, key: str) -> str:
        value: Optional[str] = keyring.get_password(self._service, key)
        if value is None:
            raise KeyError(
                f"Secret not found for service='{self._service}' key='{key}'."
            )
        return value

    def set(self, key: str, secret: str) -> None:
        keyring.set_password(self._service, key, secret)

    def exists(self, key: str) -> bool:
        try:
            return keyring.get_password(self._service, key) is not None
        except KeyringError as exc:
            raise RuntimeError(f"keyring exists failed: {exc}") from exc

    def delete(self, key: str) -> None:
        try:
            keyring.delete_password(self._service, key)
        except keyring.errors.PasswordDeleteError:
            # idempotent delete
            return
        except KeyringError as exc:
            raise RuntimeError(f"keyring delete failed: {exc}") from exc
