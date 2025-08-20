from etl_core.context.context_provider import IContextProvider
from cryptography.fernet import Fernet
import base64
import os
from typing import Any


class Credentials(IContextProvider):
    def __init__(self, credentials_id: int, user: str, password: str, database: str):
        self.credentials_id = credentials_id
        self.user = user
        self.database = database

        key = base64.urlsafe_b64encode(os.urandom(32))
        self._cipher = Fernet(key)

        self._password_encrypted = self._cipher.encrypt(password.encode())

    def __repr__(self):
        return (
            f"Credentials(credentials_id={self.credentials_id}, "
            f"user={self.user}, password=***, database={self.database})"
        )

    @property
    def password(self) -> str:
        """Decrypt and return the password securely."""
        return self._cipher.decrypt(self._password_encrypted).decode()

    def get_parameter(self, key: str) -> Any:
        """Return the value of a given parameter key."""
        mapping = {
            "credentials_id": self.credentials_id,
            "user": self.user,
            "password": self.password,
            "database": self.database,
        }
        if key not in mapping:
            raise KeyError(f"Unknown parameter key: {key}")
        return mapping[key]
