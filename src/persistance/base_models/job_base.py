from sqlmodel import SQLModel, Field
from pydantic import ConfigDict
from src.components.base_component import StrategyType


class JobBase(SQLModel):
    """
    Shared schema: used for JSON schema, validation, and as a mixin.
    """

    model_config = ConfigDict(populate_by_name=True)
    name: str = Field(default="default_job_name", nullable=False)
    num_of_retries: int = Field(default=0, nullable=False)
    file_logging: bool = Field(default=False, nullable=False)
    strategy_type: StrategyType = Field(default=StrategyType.ROW)
