from context.context_provider import IContextProvider


class Credentials(IContextProvider):
    def __init__(self, credentials_id: int, user: str, password: str, database: str):
        self.credentials_id = credentials_id
        self.user = user
        self._password = password
        self.database = database

    def __repr__(self):
        return f"Credentials(credentials_id={self.credentials_id}, user={self.user}, password=***, database={self.database})"

    @property
    def password(self):
        """Access the password securely (if needed)."""
        return self._password