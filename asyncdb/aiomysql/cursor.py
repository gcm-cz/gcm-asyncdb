from __future__ import annotations

import aiomysql
from typing import TYPE_CHECKING

from lib.asyncdb._sl import _SL

if TYPE_CHECKING:
    from .connection import AioMySQLConnection


class Cursor(aiomysql.Cursor):
    async def execute(self, query: str, args=None):
        conn: AioMySQLConnection = self._get_db()
        with _SL(2):
            try:
                if args is not None:
                    query = query % self._escape_args(args, conn)
            except Exception as exc:
                conn.observe_query_error(query, exc, conn.observe_query_before(f"{query} with args {args!r}"))
                raise

            return await super().execute(query)
