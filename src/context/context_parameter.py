class ContextParameter:
    def __init__(self, id: int, key: str, value: str, type: str, is_secure: bool):
        self.id = id
        self.key = key
        self.value = value
        self.type = type
        self.is_secure = is_secure