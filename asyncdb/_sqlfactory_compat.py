"""
Compatibility layer for sqlfactory when sqlfactory is not installed (it is optional dependency).
"""

from typing import Any

try:
    from sqlfactory.execute import ExecutableStatement
except ImportError:

    class ExecutableStatement:  # type: ignore[no-redef]  # pragma: no cover
        """
        Dummy ExecutableStatement stub for compatibility when sqlfactory is not installed.
        """

        @property
        def args(self) -> list[Any]:
            """Dummy property to simulate interface of sqlfactory's statement."""
            return []


__all__ = ["ExecutableStatement"]
