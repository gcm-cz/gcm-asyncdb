from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import aiomysql

from .._sl import _SL

if TYPE_CHECKING:
    from .connection import AioMySQLConnection  # pragma: no cover


class Cursor(aiomysql.Cursor):
    """Wrapper for Cursor class from aiomysql library that provides observer functionality for the connection."""

    async def execute(self, query: str, args: Optional[tuple[Any, ...] | list[Any] | dict[str, Any]] = None) -> int:
        conn: AioMySQLConnection = self._get_db()
        with _SL(2):
            try:
                if args is not None:
                    query = query % self._escape_args(args, conn)
            except Exception as exc:
                conn.observe_query_error(query, exc, conn.observe_query_before(f"{query} with args {args!r}"))
                raise

            return await super().execute(query)
