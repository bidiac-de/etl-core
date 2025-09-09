from sqlmodel import SQLModel
from pydantic import ConfigDict, NonNegativeInt, Field
from etl_core.components.base_component import StrategyType


class JobBase(SQLModel):
    """
    Shared schema: used for JSON schema, validation, and as a mixin.
    """

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(
        default="default_job_name",
        description="The display name of the job.",
        json_schema_extra={"order": 1},
    )
    num_of_retries: NonNegativeInt = Field(
        default=0,
        description="Number of retries to attempt if a component fails.",
        json_schema_extra={"order": 3},
    )
    file_logging: bool = Field(
        default=False,
        description="Enable or disable file-based logging for this job.",
        json_schema_extra={"order": 4},
    )
    strategy_type: StrategyType = Field(
        default=StrategyType.ROW,
        description="The execution strategy for this job (row, bulk, bigdata).",
        json_schema_extra={"order": 2},
    )
