import os
from uuid import uuid4
from typing import Optional

from etl_core.context.secrets.keyring_provider import KeyringSecretProvider
from etl_core.context.secrets.memory_provider import InMemorySecretProvider
from etl_core.context.secrets.secret_provider import SecretProvider

_MEMORY_PROVIDER_SINGLETON: Optional[InMemorySecretProvider] = None


def create_secret_provider() -> SecretProvider:
    """
    Decide the secret backend at runtime.

    Env:
      - SECRET_BACKEND: "memory" (default) or "keyring"
      - SECRET_SERVICE: service name for keyring (default "sep-sose-2025/default")
    """
    backend = os.getenv("SECRET_BACKEND", "memory").strip().lower()
    service = os.getenv("SECRET_SERVICE", "sep-sose-2025/default").strip()

    if backend == "memory":
        global _MEMORY_PROVIDER_SINGLETON
        if _MEMORY_PROVIDER_SINGLETON is None:
            _MEMORY_PROVIDER_SINGLETON = InMemorySecretProvider()
        return _MEMORY_PROVIDER_SINGLETON

    if backend == "keyring":
        return KeyringSecretProvider(service=service)

    raise ValueError(f"Unsupported SECRET_BACKEND={backend!r}")


def validate_secret_backend_configuration() -> None:
    """
    Fail fast when the configured secret backend is not operational.

    For keyring this performs a lightweight set/get/delete probe.
    """

    backend = os.getenv("SECRET_BACKEND", "memory").strip().lower()
    if backend == "memory":
        return

    provider = create_secret_provider()
    probe_key = f"etl-core-probe/{uuid4()}"
    probe_value = "ok"

    try:
        provider.set(probe_key, probe_value)
        resolved = provider.get(probe_key)
        if resolved != probe_value:
            raise RuntimeError("Secret backend probe read-back mismatch.")
    finally:
        try:
            provider.delete(probe_key)
        except Exception:
            # best-effort cleanup only
            pass
