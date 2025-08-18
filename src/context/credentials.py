from src.context.context_provider import IContextProvider


class Credentials(BaseModel, IContextProvider):
    """
    Pydantic model for database/login credentials with portable pool settings.
    Shared knobs:
      - pool_max_size: maximum connections per pool/client
      - pool_timeout_s: time to wait for a free connection (seconds)
    """

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        extra="ignore",
        validate_assignment=True,
        frozen=False,
    )

    credentials_id: int
    name: str
    user: str
    database: str
    password: Optional[SecretStr] = Field(default=None, repr=False)

    # portable pool settings for sql and mongo connections
    pool_max_size: Optional[int] = Field(default=None, ge=1)
    pool_timeout_s: Optional[int] = Field(default=None, ge=0)

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