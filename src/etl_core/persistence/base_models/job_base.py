from sqlmodel import SQLModel, Field
from pydantic import ConfigDict
from etl_core.components.strategy_type import StrategyType


class JobBase(SQLModel):
    """
    Shared schema: used for JSON schema, validation, and as a mixin.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(
        default="default_job_name",
        nullable=False,
        description="The display name of the job.",
    )
    num_of_retries: int = Field(
        default=0,
        nullable=False,
        description="Number of retries to attempt if a component fails.",
    )
    file_logging: bool = Field(
        default=False,
        nullable=False,
        description="Enable or disable file-based logging for this job.",
    )
    strategy_type: StrategyType = Field(
        default=StrategyType.ROW,
        description="The execution strategy for this job (row, bulk, bigdata).",
    )
