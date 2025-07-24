class Credentials:
    def __init__(self, credentials_id: int, user: str, password: str, database: str):
        self.credentials_id = credentials_id
        self.user = user
        self.password = password
        self.database = database