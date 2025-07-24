class ReadReceiver:
    def read_row(self, file_path: str):
        raise NotImplementedError()

    def read_bulk(self, file_path: str):
        raise NotImplementedError()

    def read_bigdata(self, file_path: str, chunk_size: int = 1000):
        raise NotImplementedError()