from pydantic import BaseModel, Field, field_validator


class FieldMapping(BaseModel):
    """
    A single mapping rule:
      - src_port: logical source port (fan-in)
      - src_path: dotted-path or column name for source.
      - dst_port: target port (fan-out).
      - dst_path: dotted-path or column name for destination.
    """

    src_port: str = Field(..., min_length=1)
    src_path: str = Field(..., min_length=1)
    dst_port: str = Field(..., min_length=1)
    dst_path: str = Field(..., min_length=1)

    @field_validator("src_path", "dst_path")
    @classmethod
    def _no_empty_segments(cls, v: str) -> str:
        if any(part.strip() == "" for part in v.split(".")):
            msg = "paths must not contain empty segments (e.g., 'a..b')"
            raise ValueError(msg)
        return v
