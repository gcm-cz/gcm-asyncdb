"""
Pydantic compatibility layer when pydantic is not installed.
"""

try:
    from pydantic import BaseModel

    HAVE_PYDANTIC: bool = True
except ImportError:

    class BaseModel:  # type: ignore[no-redef]
        """Dummy BaseModel class when pydantic is not installed. Checking issubclass against this class should always fail."""

    HAVE_PYDANTIC = False


__all__ = ["HAVE_PYDANTIC", "BaseModel"]
