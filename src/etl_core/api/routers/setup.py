from fastapi import APIRouter, Depends

from etl_core.security.dependencies import require_authorized_client

router = APIRouter(
    prefix="/setup",
    tags=["Setup"],
    dependencies=[Depends(require_authorized_client)],
)


@router.get(
    "",
    response_model=bool,
    summary="Validate secured setup access",
    description=(
        "Returns true when the caller is authenticated using the OAuth2 client "
        "credentials flow."
    ),
)
def validate_setup() -> bool:
    """Confirm that the caller holds a valid machine-to-machine access token."""

    return True
