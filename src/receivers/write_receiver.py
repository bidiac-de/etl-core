class WriteReceiver:
    def write_row(self, file_path: str, row: list):
        raise NotImplementedError()

    def write_bulk(self, file_path: str, rows: list[list]):
        raise NotImplementedError()

    def write_bigdata(self, file_path: str, data_generator):
        raise NotImplementedError()
