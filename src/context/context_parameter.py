class ContextParameter:
    def __init__(self, id: int, key: str, value: str, type: str, is_secure: bool):
        self._id = id
        self._key = key
        self._value = value
        self._type = type
        self._is_secure = is_secure

    @property
    def id(self) -> int:
        return self._id

    @id.setter
    def id(self, value: int):
        if value <= 0:
            raise ValueError("ID must be positive")
        self._id = value

    @property
    def key(self) -> str:
        return self._key

    @key.setter
    def key(self, value: str):
        if not value:
            raise ValueError("Key cannot be empty")
        self._key = value

    @property
    def value(self) -> str:
        return self._value

    @value.setter
    def value(self, val: str):
        self._value = val

    @property
    def type(self) -> str:
        return self._type

    @type.setter
    def type(self, val: str):
        self._type = val

    @property
    def is_secure(self) -> bool:
        return self._is_secure

    @is_secure.setter
    def is_secure(self, val: bool):
        self._is_secure = val
