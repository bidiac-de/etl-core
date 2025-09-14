from __future__ import annotations


class PersistenceError(Exception):
    """Base for all persistence-layer exceptions."""


class PersistNotFoundError(PersistenceError):
    """Requested row or resource was not found."""


class PersistValidationError(PersistenceError):
    """Config/model validation failed before touching the DB."""


class PersistLinkageError(PersistenceError):
    """Invalid graph/link references (e.g., unknown `next` component)."""


class PersistConflictError(PersistenceError):
    """Constraint/uniqueness violation or similar conflict."""


class PersistOperationalError(PersistenceError):
    """Connection issues, timeouts, transaction or generic DB failures."""
