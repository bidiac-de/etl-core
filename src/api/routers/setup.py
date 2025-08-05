from typing import Final

from fastapi import APIRouter

router = APIRouter(
    prefix="/setup",
    tags=["setup"],
)

VALID_KEY: Final[str] = "VALID_KEY"


@router.get(
    "",
    response_model=bool,
    summary="Validate GUI setup key",
    description="Return true if the provided key matches the preset valid key.",
)
def validate_key(key: str) -> bool:
    """
    Dummy endpoint for GUI setup: checks if the incoming key
    matches the VALID_KEY constant.
    """
    return key == VALID_KEY
