from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Optional

from src.context.context import Context
from src.context.context_parameter import ContextParameter
from src.context.credentials import Credentials
from src.context.context_provider import IContextProvider
from src.context.secrets.secret_provider import SecretProvider


@dataclass(frozen=True)
class BootstrapResult:
    stored: list[str]
    skipped_existing: list[str]
    errors: dict[str, str]


class SecureContextAdapter(IContextProvider):
    """
    Wraps a Context or Credentials and resolves secure params via SecretProvider.
    Strategy:
      - For Context: parameters[key].is_secure -> read from secret store.
      - For Credentials: treat 'password' as secure; other fields plain.
    """

    def __init__(
        self,
        *,
        provider_id: str,
        secret_store: SecretProvider,
        context: Optional[Context] = None,
        credentials: Optional[Credentials] = None,
    ) -> None:
        if not context and not credentials:
            raise ValueError("Provide either `context` or `credentials`.")
        self._provider_id = provider_id
        self._secret_store = secret_store
        self._context = context
        self._credentials = credentials

    # lifecycle
    def bootstrap_to_store(self) -> BootstrapResult:
        """
        Persist secure values to the secret store, verify writes, and blank
        in-memory context values that were actually stored.
        """
        result = BootstrapResult(stored=[], skipped_existing=[], errors={})
        self._bootstrap_context_params(result)
        self._bootstrap_credentials(result)
        return result

    def _bootstrap_context_params(self, result: BootstrapResult) -> None:
        if not self._context:
            return

        for key, param in self._context.parameters.items():
            if getattr(param, "is_secure", False) and param.value:
                self._store_secret(result, key, param.value)

        # Blank only keys that we actually persisted for this context
        for key in result.stored:
            if key in self._context.parameters:
                self._context.parameters[key].value = ""

    def _bootstrap_credentials(self, result: BootstrapResult) -> None:
        if not self._credentials:
            return

        pwd = self._credentials.decrypted_password
        if pwd:
            self._store_secret(result, "password", pwd)

    def _store_secret(self, result: BootstrapResult, key: str, plaintext: str) -> None:
        try:
            skey = self._secret_key(key)
            if self._secret_store.exists(skey):
                result.skipped_existing.append(key)
                return

            self._secret_store.set(skey, plaintext)

            # Read-back verification
            if self._secret_store.get(skey) != plaintext:
                raise RuntimeError("verification failed")

            result.stored.append(key)
        except Exception as exc:  # noqa: BLE001
            result.errors[key] = f"{type(exc).__name__}: {exc}"

    def delete_from_store(self) -> None:
        """
        Delete only the keys this adapter is responsible for.
        Idempotent: ignores missing keys.
        """
        if self._context:
            for k, p in self._context.parameters.items():
                if p.is_secure:
                    try:
                        self._secret_store.delete(self._secret_key(k))
                    except Exception:
                        pass  # best-effort cleanup
        if self._credentials:
            try:
                self._secret_store.delete(self._secret_key("password"))
            except Exception:
                pass

    # IContextProvider

    def get_parameter(self, key: str) -> Any:
        # Context path
        if self._context:
            param: ContextParameter = self._context.parameters[key]
            if param.is_secure:
                return self._secret_store.get(self._secret_key(key))
            return param.value
        # Credentials path
        if self._credentials:
            if key == "password":
                return self._secret_store.get(self._secret_key("password"))
            return self._credentials.get_parameter(key)
        raise RuntimeError("SecureContextAdapter not initialized correctly.")

    # helpers

    def _secret_key(self, key: str) -> str:
        return f"{self._provider_id}/{key}"

    @property
    def provider_id(self) -> str:
        return self._provider_id

    @property
    def context(self) -> Optional[Context]:
        return self._context

    @property
    def credentials(self) -> Optional[Credentials]:
        return self._credentials
