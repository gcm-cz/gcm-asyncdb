"""
Compatibility layer for sqlfactory when sqlfactory is not installed (it is optional dependency).
"""
from typing import Any

try:
    from sqlfactory.execute import ExecutableStatement
except ImportError:
    class ExecutableStatement:
        """
        Dummy ExecutableStatement stub for compatibility when sqlfactory is not installed.
        """

        @property
        def args(self) -> list[Any]:
            return []
