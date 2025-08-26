from pydantic import model_validator

class ReadXML(XML):
    type: Literal["read_xml"] = "read_xml"

    @model_validator(mode="after")
    def _build_objects(self):
        # pass schema to receiver so it can validate on the fly
        self._receiver = XMLReceiver(schema=self.schema)
        return self

    async def process_row(
            self, row: Dict[str, Any], metrics: Any
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async for rec in self._receiver.read_row(
                self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        ):
            yield rec

    async def process_bulk(self, data: Any, metrics: Any) -> AsyncGenerator[pd.DataFrame, None]:
        df = await self._receiver.read_bulk(
            self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield df

    async def process_bigdata(self, chunk_iterable: Any, metrics: Any) -> AsyncGenerator[dd.DataFrame, None]:
        ddf = await self._receiver.read_bigdata(
            self.filepath, metrics=metrics, root_tag=self.root_tag, record_tag=self.record_tag
        )
        yield ddf


