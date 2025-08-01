from context.context_provider import IContextProvider
from cryptography.fernet import Fernet
import base64
import os


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