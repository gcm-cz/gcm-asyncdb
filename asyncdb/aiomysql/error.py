import asyncio

import aiomysql
from pymysql import InterfaceError
from pymysql.constants import CR, ER

from ..exceptions import (
    Deadlock,
    DuplicateEntry,
    LockWaitTimeout,
    LostConnection,
    ParseError,
    QueryError,
    QueryTooBig,
    ServerGone,
)
from ..exceptions import (
    SyntaxError as QuerySyntaxError,
)
from .connection import AioMySQLConnection


def _query_error_factory(query: str, exc: Exception, conn: AioMySQLConnection) -> None:
    if isinstance(exc, InterfaceError):
        # This is pretty lousy, but unfortunately aiomysql hides all the errors into aiomysql.Error, so we have no way
        # how to catch task cancellation. Unfortunately not event exc.__cause__ is filled in, so comparing to exception
        # message is currently the only way.
        if str(exc) == "Cancelled during execution":
            raise asyncio.CancelledError()

    if isinstance(exc, aiomysql.Error):
        clz = {
            ER.DUP_ENTRY: DuplicateEntry,
            ER.LOCK_DEADLOCK: Deadlock,
            ER.PARSE_ERROR: ParseError,
            ER.SYNTAX_ERROR: QuerySyntaxError,
            ER.LOCK_WAIT_TIMEOUT: LockWaitTimeout,
            CR.CR_SERVER_GONE_ERROR: ServerGone,
            CR.CR_SERVER_LOST: LostConnection,
            CR.CR_NET_PACKET_TOO_LARGE: QueryTooBig,
        }.get(exc.args[0], QueryError)

        raise clz(
            query,
            exc.args[1] if len(exc.args) > 1 else str(exc),
            exc.args[0],
            remote_app=conn.remote_app,
        )

    raise exc
