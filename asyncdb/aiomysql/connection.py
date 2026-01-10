from __future__ import annotations

import datetime
from asyncio import AbstractEventLoop
from collections.abc import Callable, Collection
from ssl import SSLContext
from typing import Any

import aiomysql
from pymysql.constants import COMMAND, FIELD_TYPE
from pymysql.converters import convert_datetime, decoders

from .. import logger
from .._sl import _SL
from .observer import ObserverContext, QueryObserver, _QueryObserverContext
from .observers.logging_observer import LoggingObserver


class AioMySQLConnection(aiomysql.Connection):
    """
    Wrapper for aiomysql.Connection that allows observing queries, correctly sets timeouts, and provides timezone support.

    Usually in most applications, you don't want to initialize the connection on its own, you want to use connection pool
    instead, to be able to handle multiple operations simultaneously. See `TransactionFactory` for more apropriate usage.

    The connection itself is suitable for one-time jobs (cronjobs), where concurrency is usually not used.

    Example:

    >>> import asyncio
    >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
    >>>
    >>> # Create the connection.
    >>> conn = AioMySQLConnection(host="localhost", user="root", db="test")
    >>>
    >>> async def main():
    >>>     # Open transaction on the connection. If the connection has not been connected, it is connected at first use here.
    >>>     async with Transaction(conn) as trx:
    >>>         # Perform normal operations on the transaction.
    >>>         res = await trx.query("SELECT `id`, `name` FROM `products`")
    >>>         # Don't forge to commit the transaction, otherwise implicit rollback will be performed.
    >>>         await trx.commit()
    >>>
    >>> if __name__ == "__main__":
    >>>     asyncio.run(main())
    """

    def __init__(
        self,
        host: str = "localhost",
        user: str | None = None,
        password: str = "",
        db: str | None = None,
        port: int = 3306,
        unix_socket: str | None = None,
        charset: str = "",
        sql_mode: str | None = None,
        read_default_file: str | None = None,
        conv: dict[int, Callable[[str | bytes], Any]] | None = None,
        use_unicode: bool | None = None,
        client_flag: int = 0,
        init_command: str | None = None,
        connect_timeout: int | None = None,
        read_default_group: int | None = None,
        autocommit: bool = False,
        echo: bool = False,
        local_infile: bool = False,
        loop: AbstractEventLoop | None = None,
        ssl: SSLContext | None = None,
        auth_plugin: str = "",
        program_name: str = "",
        server_public_key: str | None = None,
        remote_app: str = "",
        timezone: datetime.tzinfo | None = datetime.UTC,
        read_timeout: int | None = None,
        write_timeout: int | None = None,
        wait_timeout: int | None = None,
    ) -> None:
        # pylint: disable=too-many-arguments, too-many-positional-arguments, too-many-locals
        """
        Establish a connection to the MySQL database. Accepts several arguments:

        :param host: Hostname of the database to connect to.
        :param user: Username to authenticate as.
        :param password: Password to authenticate with.
        :param db: Database to use.
        :param port: TCP port of the MySQL server.
        :param unix_socket: Optionally, you can use a unix socket rather than TCP/IP.
        :param charset: Charset you want to use.
        :param sql_mode: Default SQL_MODE to use.
        :param read_default_file: Specifies my.cnf file to read these parameters from under the [client] section.
        :param conv: Decoders dictionary to use instead of the default one. This is used to provide custom marshalling of types.
        :param use_unicode: Whether or not to default to unicode strings.
        :param client_flag: Custom flags to send to MySQL. Find potential values in pymysql.constants.CLIENT.
        :param init_command: Initial SQL statement to run when connection is established.
        :param connect_timeout: Timeout before throwing an exception when connecting.
        :param read_default_group: Group to read from in the configuration file.
        :param autocommit: Autocommit mode. None means use server default. (default: False)
        :param echo:
        :param local_infile: boolean to enable the use of LOAD DATA LOCAL command. (default: False)
        :param loop: asyncio loop to use for the connection.
        :param ssl: Optional SSL context to force SSL.
        :param auth_plugin: String to manually specify the authentication plugin to use, i.e you will want to use
            mysql_clear_password when using IAM authentication with Amazon RDS. (default: Server default)
        :param program_name: Program name string to provide when handshaking with MySQL. (omitted by default)
        :param server_public_key: SHA256 authentication plugin public key value. (default: None)
        :param remote_app: Identifier of the connection. Used for logging and is populated to exceptions.
        :param timezone: Timezone configured for the connection. It is set as connection attribute, so database will know
            what timezone it should accept. Also it is used to populate the tzinfo attribute of datetime objects.
            When None, naive datetimes are used and no timezone information is sent to the database. (default: UTC)
        :param read_timeout: Timeout (in seconds) for waiting for MySQL to process query and return result. (default: None)
        :param write_timeout: Timeout (in seconds) for sending data to MySQL server (default: None)
        :param wait_timeout: Timeout (in seconds) for closing the connection when idle. (default: None)
        """

        if conv is None:
            conv = decoders.copy()

        if timezone is not None:
            # Produce timezone-aware datetimes with specified timezone information.
            def convert_datetime_with_tz(obj: Any) -> Any:
                converted = convert_datetime(obj)
                if isinstance(converted, datetime.datetime):
                    return converted.replace(tzinfo=timezone)

                return converted

            conv[FIELD_TYPE.DATETIME] = convert_datetime_with_tz
            conv[FIELD_TYPE.TIMESTAMP] = convert_datetime_with_tz

            # Set correct timezone for the connection too.
            offset = timezone.utcoffset(datetime.datetime.now())  # noqa: DTZ005  # We want local time here.
            if offset is not None:
                hours = offset.seconds // 3600
                minutes = (offset.seconds // 60) % 60

                init_command = f"SET time_zone='{hours:+03d}:{minutes:02d}'; {init_command or ''}"

        if read_timeout is not None:
            init_command = f"SET net_read_timeout={read_timeout}; {init_command or ''}"

        if write_timeout is not None:
            init_command = f"SET net_write_timeout={write_timeout}; {init_command or ''}"

        if wait_timeout is not None:
            init_command = f"SET wait_timeout={wait_timeout}; {init_command or ''}"

        super().__init__(
            host=host,
            user=user,
            password=password,
            db=db,
            port=port,
            unix_socket=unix_socket,
            charset=charset,
            sql_mode=sql_mode,
            read_default_file=read_default_file,
            conv=conv,
            use_unicode=use_unicode,
            client_flag=client_flag,
            init_command=init_command,
            connect_timeout=connect_timeout,
            read_default_group=read_default_group,
            autocommit=autocommit,
            echo=echo,
            local_infile=local_infile,
            loop=loop,
            ssl=ssl,
            auth_plugin=auth_plugin,
            program_name=program_name,
            server_public_key=server_public_key,
        )

        self.timezone = timezone
        """
        Timezone configured for the connection. It is used to populate the tzinfo attribute of datetime objects.

        Note that changing this during runtime won't affect the connection's timezone, but will affect tzinfo attribute
        of datetimes returned in the result sets.
        """

        self.logger = logger.getChild(f"mysql.{self.db}") if self.db else logger.getChild("mysql")
        """Logger for this database."""

        self.observers: set[QueryObserver] = {LoggingObserver(self.logger)}
        """Observers for this connection. By default, LoggingObserver is always present."""

        self.remote_app = remote_app or f"{self._user}@{self._host}:{self._port}/{self._db}"
        """Identifier of the connection. Used for logging and is populated to exceptions."""

    def attach(self, observer: QueryObserver) -> None:
        """Attach new observer instance to the connection."""
        self.observers.add(observer)

    def detach(self, observer: QueryObserver) -> None:
        """Detach observer instance from the connection."""
        self.observers.remove(observer)

    async def _connect(self) -> None:
        with _SL(1):
            await super()._connect()

    def escape(self, obj: Any) -> str:
        """Escape a value to be used in a query."""

        # Encode timezone-aware datetime with correct timezone.
        if isinstance(obj, datetime.datetime) and self.timezone is not None and obj.tzinfo is not None:
            obj = obj.astimezone(self.timezone).strftime("%Y-%m-%d %H:%M:%S.%f")

        return super().escape(obj)

    def observe_query_before(self, sql: str, *, query_pattern: str, query_args: Collection[Any], **kwargs: Any) -> dict[int, Any]:
        """
        Callback called before query execution.
        :param sql: SQL query
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer
        :return: Results from the observer callbacks, that are passed to the after and error callbacks.
        """
        observer_context: dict[int, Any] = {}

        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer_context[id(observer)] = observer.observe_query_before(
                        self, sql, query_pattern=query_pattern, query_args=query_args, **ctx.kwargs()
                    )

            return observer_context

    def observe_query_after(
        self,
        sql: str,
        observer_context: dict[int, Any],
        *,
        query_pattern: str,
        query_args: Collection[Any],
        **kwargs: Any,
    ) -> None:
        """
        Callback called after query execution.
        :param sql: SQL query
        :param observer_context: Context from the before callback
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_after(
                        self,
                        sql,
                        observer_context.get(id(observer)),
                        query_pattern=query_pattern,
                        query_args=query_args,
                        **ctx.kwargs(),
                    )

    def observe_query_error(
        self,
        sql: str,
        exc: BaseException,
        observer_context: dict[int, Any],
        *,
        query_pattern: str,
        query_args: Collection[Any],
        **kwargs: Any,
    ) -> None:
        """
        Callback called when query execution raises an exception.
        :param sql: SQL query
        :param exc: Exception raised
        :param observer_context: Context from the before callback
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_error(
                        self,
                        sql,
                        exc,
                        observer_context.get(id(observer)),
                        query_pattern=query_pattern,
                        query_args=query_args,
                        **ctx.kwargs(),
                    )

    async def _execute_command(self, command: int, sql: str | bytes) -> None:
        """Override aiomysql's _execute_command to provide query observing."""
        with _SL():
            if command == COMMAND.COM_QUERY:
                ctx = _QueryObserverContext.query_ctx.get()
                if ctx is not None:
                    ctx.sql = sql.decode("utf-8", "backslashreplace") if isinstance(sql, bytes) else sql
                    ctx.observer_context = self.observe_query_before(
                        ctx.sql, query_pattern=ctx.query_pattern, query_args=ctx.query_args, **ctx.kwargs()
                    )

            await super()._execute_command(command, sql)

    async def query(self, sql: str, unbuffered: bool = False, **kwargs: Any) -> int:
        """Override aiomysql's query to provide query observing."""
        with _SL(3):
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
