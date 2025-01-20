from __future__ import annotations

from asyncio import AbstractEventLoop
from ssl import SSLContext
from typing import Any, Callable, Optional, Type

import aiomysql
from pymysql.constants import COMMAND
from pymysql.converters import decoders

from .. import logger
from .._sl import _SL
from .cursor import Cursor
from .observer import LoggingObserver, ObserverContext, QueryObserver, _QueryObserverContext


class AioMySQLConnection(aiomysql.Connection):
    """Wrapper for aiomysql.Connection that allows observing queries."""

    def __init__(
        self,
        host: str = "localhost",
        user: Optional[str] = None,
        password: str = "",
        db: Optional[str] = None,
        port: int = 3306,
        unix_socket: Optional[str] = None,
        charset: str = "",
        sql_mode: Optional[str] = None,
        read_default_file: Optional[str] = None,
        conv: Optional[dict[int, Callable[[str | bytes], Any]]] = None,
        use_unicode: Optional[bool] = None,
        client_flag: int = 0,
        cursorclass: Type[Cursor] = Cursor,
        init_command: Optional[str] = None,
        connect_timeout: Optional[int] = None,
        read_default_group: Optional[int] = None,
        autocommit: bool = False,
        echo: bool = False,
        local_infile: bool = False,
        loop: Optional[AbstractEventLoop] = None,
        ssl: Optional[SSLContext] = None,
        auth_plugin: str = "",
        program_name: str = "",
        server_public_key: Optional[str] = None,
        remote_app: str = "",
    ) -> None:
        # pylint: disable=too-many-arguments, too-many-positional-arguments, too-many-locals
        super().__init__(
            host,
            user,
            password,
            db,
            port,
            unix_socket,
            charset,
            sql_mode,
            read_default_file,
            conv if conv is not None else decoders,
            use_unicode,
            client_flag,
            cursorclass,
            init_command,
            connect_timeout,
            read_default_group,
            autocommit,
            echo,
            local_infile,
            loop,
            ssl,
            auth_plugin,
            program_name,
            server_public_key,
        )

        self.logger = logger.getChild(f"mysql.{self.db}") if self.db else logger.getChild("mysql")
        self.observers: set[QueryObserver] = {LoggingObserver(self.logger)}
        self.remote_app = remote_app or f"{self._user}@{self._host}:{self._port}/{self._db}"

    def attach(self, observer: QueryObserver) -> None:
        """Attach new observer instance to the connection."""
        self.observers.add(observer)

    def detach(self, observer: QueryObserver) -> None:
        """Detach observer instance from the connection."""
        self.observers.remove(observer)

    def observe_query_before(self, sql: str, **kwargs: Any) -> dict[int, Any]:
        """
        Callback called before query execution.
        :param sql: SQL query
        :param kwargs: Additional kwargs passed to the observer
        :return: Results from the observer callbacks, that are passed to the after and error callbacks.
        """
        observer_context: dict[int, Any] = {}

        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer_context[id(observer)] = observer.observe_query_before(self, sql, **ctx.kwargs())

            return observer_context

    def observe_query_after(self, sql: str, observer_context: dict[int, Any], **kwargs: Any) -> None:
        """
        Callback called after query execution.
        :param sql: SQL query
        :param observer_context: Context from the before callback
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_after(self, sql, observer_context.get(id(observer)), **ctx.kwargs())

    def observe_query_error(self, sql: str, exc: BaseException, observer_context: dict[int, Any], **kwargs: Any) -> None:
        """
        Callback called when query execution raises an exception.
        :param sql: SQL query
        :param exc: Exception raised
        :param observer_context: Context from the before callback
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_error(self, sql, exc, observer_context.get(id(observer)), **ctx.kwargs())

    async def _execute_command(self, command: int, sql: str | bytes) -> None:
        """Override aiomysql's _execute_command to provide query observing."""
        with _SL():
            if command == COMMAND.COM_QUERY:
                ctx = _QueryObserverContext.query_ctx.get()
                if ctx is not None:
                    ctx.sql = sql.decode("utf-8") if isinstance(sql, bytes) else sql
                    ctx.observer_context = self.observe_query_before(ctx.sql, **ctx.kwargs())

            await super()._execute_command(command, sql)

    async def query(self, sql: str, unbuffered: bool = False, **kwargs: Any) -> int:
        """Override aiomysql's query to provide query observing."""
        with _SL(4):
            with _QueryObserverContext(self, **kwargs):
                return await super().query(sql, unbuffered)

    async def _send_autocommit_mode(self, **kwargs: Any) -> None:
        """Override aiomysql's _send_autocommit_mode to provide query observing."""
        with _SL(4):
            with _QueryObserverContext(self, **kwargs):
                await super()._send_autocommit_mode()

    async def begin(self, **kwargs: Any) -> None:
        """Override aiomysql's begin to provide query observing."""
        with _SL():
            with _QueryObserverContext(self, **kwargs):
                await super().begin()

    async def commit(self, **kwargs: Any) -> None:
        """Override aiomysql's commit to provide query observing."""
        with _SL(3):
            with _QueryObserverContext(self, **kwargs):
                await super().commit()

    async def rollback(self, **kwargs: Any) -> None:
        """Override aiomysql's rollback to provide query observing."""
        with _SL(3):
            with _QueryObserverContext(self, **kwargs):
                await super().rollback()

    async def show_warnings(self, **kwargs: Any) -> None | tuple[Any, ...]:
        """Override aiomysql's show_warnings to provide query observing."""
        with _QueryObserverContext(self, **kwargs):
            return await super().show_warnings()

    async def set_charset(self, charset: str, **kwargs: Any) -> None:
        """Override aiomysql's set_charset to provide query observing."""
        with _QueryObserverContext(self, **kwargs):
            await super().set_charset(charset)

    def __repr__(self) -> str:
        return f"<aiomysql.Connection 0x{id(self):x} {self.remote_app}>"


AioMySQLConnection.__init__.__doc__ = aiomysql.Connection.__init__.__doc__
