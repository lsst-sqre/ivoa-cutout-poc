"""Utility functions for services using UWS."""

from __future__ import annotations

from datetime import datetime, timezone


def isodatetime(timestamp: datetime) -> str:
    """Format a timestamp in UTC in the expected UWS ISO date format."""
    assert timestamp.tzinfo in (None, timezone.utc)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_isodatetime(time_string: str) -> datetime | None:
    """Parse a string in the UWS ISO date format.

    Returns
    -------
    datetime.datetime or None
        The corresponding `datetime.datetime` or `None` if the string is
        invalid.
    """
    if not time_string.endswith("Z"):
        return None
    try:
        return datetime.fromisoformat(time_string[:-1] + "+00:00")
    except Exception:
        return None


def validate_isodatetime(v: str | None) -> datetime | None:
    """Validate an input date that should be in ISO format.

    Intended for use as a Pydantic validator.

    Parameters
    ----------
    v
        Input date, which may be `None`.

    Returns
    -------
    datetime.datetime or None
        Corresponding `datetime.datetime` or `None` if the input was `None`.

    Raises
    ------
    ValueError
        The input was not a string in the expected ISO format.
    """
    if v is None:
        return None
    if not isinstance(v, str) or not v.endswith("Z"):
        raise ValueError("Must be a string in YYYY-MM-DDTHH:MM[:SS]Z format")
    try:
        return datetime.fromisoformat(v[:-1] + "+00:00")
    except Exception as e:
        raise ValueError(f"Invalid date {v}: {str(e)}") from e
