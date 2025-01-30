"""
Connection pool (not only) for databases.
"""

from __future__ import annotations

import asyncio
from asyncio import CancelledError, Queue, QueueEmpty, Task
from collections.abc import Iterable
from contextlib import asynccontextmanager
from datetime import datetime
from time import time
from types import TracebackType
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    ClassVar,
    Generic,
    Optional,
    Self,
    Type,
    TypeVar,
    cast,
    overload,
)

import wrapt

from . import logger
from ._sl import _SL
from .asyncdb import Result, Transaction
from .exceptions import UnableToCreateConnection
from .generics import OBJ, OBJ_OR_FACTORY, ConnectionTypeT, DictT, ListT, ResultRow

if TYPE_CHECKING:
    from prometheus_client import Gauge


class CountedConnection(Generic[ConnectionTypeT], wrapt.ObjectProxy):
    # pylint: disable=abstract-method

    """
    @private

    Helper class that is injected to each connection to be able to track usage of the connection to provide lifecycle
    to the pool.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        :param args: Arguments passed to the actual connection constructor.
        :param kwargs: Keyword argumens passed to the actual connection constructor.
        """
        super().__init__(*args, **kwargs)

        self.used: int = 0
        self.created: float = time()
        self.current_transaction: Optional[Transaction[ConnectionTypeT]] = None

    def close(self) -> None | Awaitable[None]:
        """
        Close the connection. This is here to provide predictable interface, as connection class might not
        provide close() method.
        """
        if hasattr(super(), "close"):
            return super().close()

        return None

    def __repr__(self) -> str:
        """
        Add used and created fields to the actual connection representation.
        """
        return f"{self.__wrapped__!r} used={self.used}, created={datetime.fromtimestamp(self.created)}"  # noqa: DTZ006


class PooledTransaction(Generic[ConnectionTypeT], wrapt.ObjectProxy):
    # pylint: disable=abstract-method

    """
    @private

    Helper class that is injected to each transaction to return transaction to the pool when not used anymore.
    """

    def __init__(self, *args: Any, pool: "ConnectionPool[ConnectionTypeT]", **kwargs: Any) -> None:
        """
        :param connection: Connection for this transaction
        :param pool: Connection pool this transaction has been spinned from
        """
        super().__init__(*args, **kwargs)
        self.pool = pool

    async def __aenter__(self) -> Self:
        if hasattr(self.__wrapped__, "__aenter__"):
            with _SL():
                await self.__wrapped__.__aenter__()

        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """
        Return connection to the pool when the transaction's context is left.
        """
        try:
            if hasattr(self.__wrapped__, "__aexit__"):
                with _SL(2):
                    await self.__wrapped__.__aexit__(exc_type, exc_val, exc_tb)

            cast(CountedConnection[ConnectionTypeT], self.connection).current_transaction = None
        finally:
            await self.pool.return_conn(cast(CountedConnection[ConnectionTypeT], self.connection))

    if TYPE_CHECKING:

        async def is_committed(self) -> bool:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Return True if transaction is committed.
            """
            ...

        async def last_insert_id(self) -> int:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Return last used AUTO_INCREMENT value.
            """
            ...

        async def query(self, query: str, *args: Any, **kwargs: Any) -> Result:
            # pylint: disable=unused-argument, unnecessary-ellipsis, no-self-use
            """
            Execute single query inside the transaction scope.
            :param query: Query to execute. Can contain %s for each arg from args.
            :param args: Arguments to the query
            :param kwargs: Some drivers supports also specifying keyword arguments for the query, but most does not.
            :return: Result if query returns rows, or True if query succeeded.
            :raises: QueryError (or subclass of) if there was an error processing the query.
            """
            ...

        async def affected_rows(self) -> int:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Return number of affected rows by most recent query.
            """
            ...

        async def commit(self) -> None:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Commit the transaction. After commit, no other operation can be performed on the transaction.
            :raises: QueryError (or subclass of) if there was an error during commit.
            """
            ...

        async def rollback(self) -> None:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Rollback the transaction. After rollback, no other operation can be performed on the transaction.
            :raises: QueryError (or subclass of) if there was an error during rollback.
            """
            ...

        async def _get(
            self,
            result_fetcher: Callable[..., Callable[..., Awaitable[Any]]],
            query: str,
            args: Optional[Iterable[Any]],
            cls: Type[T] | Callable[..., T],
            *cls_args: Any,
            **cls_kwargs: Any,
        ) -> T:
            # pylint: disable=unused-argument, no-self-use
            ...

        @overload
        async def get_object(self, query: str, args: Optional[Iterable[Any]] = None, /) -> ResultRow: ...

        @overload
        async def get_object(
            self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[OBJ] = ..., /, *cls_args: Any, **cls_kwargs: Any
        ) -> OBJ: ...

        @overload
        async def get_object(
            self,
            query: str,
            args: Optional[Iterable[Any]] = None,
            cls: Callable[..., OBJ] = ...,
            /,
            *cls_args: Any,
            **cls_kwargs: Any,
        ) -> OBJ: ...

        async def get_object(
            self,
            query: str,
            args: Optional[Iterable[Any]] = None,
            cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow,
            /,
            *cls_args: Any,
            **cls_kwargs: Any,
        ) -> OBJ | ResultRow:
            # pylint: disable=unused-argument, unnecessary-ellipsis, no-self-use
            """
            Retrieve single object from the database. The query must return exactly one row, which will be
            converted to object using standard Result.fetch_object.
            :param query: Query to execute
            :param args: Arguments to the query
            :param cls: See Result.fetch_object for details
            :param cls_args: See Result.fetch_object for details
            :param cls_kwargs: See Result.fetch_object for details
            :return: Object retrieved by the query.
            :raises: LogicError if query did not return a result set (i.e. modifying query instead of SELECT).
            :raises: EntityNotFound if query returned a result set, but it was empty.
            :raises: TooManyRowsError if query returned more than single row.
            """
            ...

        @overload
        async def get_dict(self, query: str, args: Optional[Iterable[Any]] = None, /) -> dict[str, Any]: ...

        @overload
        async def get_dict(
            self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
        ) -> DictT: ...

        async def get_dict(
            self,
            query: str,
            args: Optional[Iterable[Any]] = None,
            cls: Type[DictT | dict[str, Any]] = dict,
            /,
            *cls_args: Any,
            **cls_kwargs: Any,
        ) -> DictT | dict[str, Any]:
            # pylint: disable=unused-argument, unnecessary-ellipsis, no-self-use
            """
            Retrieve single row from the database as dict. The query must return exactly one row, which will be
            converted to dict using standard Result.fetch_dict.
            :param query: Query to execute
            :param args: Arguments to the query
            :param cls: See Result.fetch_dict for details
            :param cls_args: See Result.fetch_dict for details
            :param cls_kwargs: See Result.fetch_dict for details
            :return: Dict retrieved by the query.
            :raises: LogicError if query did not return a result set (i.e. modifying query instead of SELECT).
            :raises: EntityNotFound if query returned a result set, but it was empty.
            :raises: TooManyRowsError if query returned more than single row.
            """
            ...

        @overload
        async def get_list(self, query: str, args: Optional[Iterable[Any]] = None, /) -> list[Any]: ...

        @overload
        async def get_list(
            self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
        ) -> ListT: ...

        async def get_list(
            self,
            query: str,
            args: Optional[Iterable[Any]] = None,
            cls: Type[ListT | list[Any]] = list,
            /,
            *cls_args: Any,
            **cls_kwargs: Any,
        ) -> ListT | list[Any]:
            # pylint: disable=unused-argument, unnecessary-ellipsis, no-self-use
            """
            Retrieve single row from the database as list. The query must return exactly one row, which will be
            converted to list using standard Result.fetch_list.
            :param query: Query to execute
            :param args: Arguments to the query
            :param cls: See Result.fetch_list for details
            :param cls_args: See Result.fetch_list for details
            :param cls_kwargs: See Result.fetch_list for details
            :return: List retrieved by the query.
            :raises: LogicError if query did not return a result set (i.e. modifying query instead of SELECT).
            :raises: EntityNotFound if query returned a result set, but it was empty.
            :raises: TooManyRowsError if query returned more than single row.
            """
            ...

        async def get_scalar(self, query: str, args: Optional[Iterable[Any]] = None) -> Any:
            # pylint: disable=unused-argument, unnecessary-ellipsis, no-self-use
            """
            Retrieve single value from the database. The query must return exactly one row and that
            row must contain exactly one column, which is then returned from this method.
            :param query: Query to execute
            :param args: Query arguments.
            :return: Value of first column of the first row of the result set.
            :raises: LogicError if query did not return a result set (i.e. modifying query instead of SELECT).
            :raises: EntityNotFound if query returned a result set, but it was empty.
            :raises: TooManyRowsError if query returned more than single row or a row contains more than single column.
            """
            ...

        @property
        def connection(self) -> ConnectionTypeT:
            # pylint: disable=unnecessary-ellipsis
            """
            Return connection that this transaction is open on.
            :return: connection object
            """
            ...

        async def cleanup(self) -> None:
            # pylint: disable=unnecessary-ellipsis, no-self-use
            """
            Cleanup the connection associated with this transaction and prepare it for usage by next transaction.
            """
            ...


class PoolMetrics:
    """
    Metrics for the connection pool.
    """

    idle_connections: Optional[Gauge] = None
    used_connections: Optional[Gauge] = None
    connection_limit: Optional[Gauge] = None


class ConnectionPool(Generic[ConnectionTypeT]):  # pylint: disable=too-many-instance-attributes
    """
    Generic connection pool for anything. Not related directly to the database. The database connection pool is just a special
    case of generic connection pool.
    """

    def __init__(
        self,
        connection_class: Type[ConnectionTypeT],
        *args: Any,
        max_pool_size: int = 100,
        max_spare_conns: int = 10,
        min_spare_conns: int = 5,
        max_conn_lifetime: int = 300,
        max_conn_usage: int = 100,
        keeper_task: bool = True,
        **kwargs: Any,
    ) -> None:
        """
        :param connection_class: Class used to make new connections. When creating connection, args and kwargs are
          passed to the class. Also, you can override create_conn() method to provide initialization logic by yourself.
        :param args: Arguments to connection constructor.
        :param max_pool_size: Maximum number of connection this pool can contain (including currently used ones).
        :param max_spare_conns: Maximum number of spare (unused) connections this pool will hold. Any excessive
          connections are terminated.
        :param min_spare_conns: Minimum number of spare (unused) connections this pool will hold. If number of available
          connections drop below min_spare_conns, new connection is created up to max_pool_size.
        :param max_conn_lifetime: Maximum number of seconds one connection can be kept alive before terminating and
          replacing it with fresh one.
        :param max_conn_usage: Maximum number of usages each connection can have before terminating and replacing
          it with fresh one.
        :param keeper_task: Should the keeper task be started automatically?
        :param kwargs: Optional keyword arguments passed to connection constructor.
        """
        max_spare_conns = min(max_spare_conns, max_pool_size)
        min_spare_conns = min(min_spare_conns, max_spare_conns)

        if max_conn_lifetime < 0:
            raise AttributeError("max_conn_lifetime must be greater or equal to zero")

        if max_conn_usage < 0:
            raise AttributeError("max_conn_usage must be greater or equal to zero")

        self.connection_class = connection_class
        """Class used to make new connections."""
        self.args = args
        """Arguments passed to connection_class constructor."""
        self.kwargs = kwargs
        """Keyword arguments passed to connection_class constructor."""

        self.log = logger.getChild("pool")
        """Logger for the pool related messages."""

        self.loop = kwargs.get("loop", asyncio.get_running_loop())
        """Connection pool used for the loop. Defaults to currently running event loop."""

        self.max_pool_size = max_pool_size
        """Maximum number of connections this pool can contain at any time, including used ones."""

        self.max_spare_conns = max_spare_conns
        """Maximum number of spare connections this pool will hold."""

        self.min_spare_conns = min_spare_conns
        """Minimum number of spare connections this pool will hold."""

        self.max_conn_lifetime = max_conn_lifetime
        """Maximum number of seconds one connection can be kept alive before terminating and replacing it with fresh one."""

        self.max_conn_usage = max_conn_usage
        """Maximum number of usages each connection can have before terminating and replacing it with fresh one."""

        self.queue = Queue[CountedConnection[ConnectionTypeT]]()
        """Queue holding the available (spare) connections."""

        self._connections_in_use = 0

        self._keeper_task_done: Optional[Task[None]] = None
        if keeper_task:
            self._start_keeper_task()

        self.metrics = PoolMetrics()
        """Default metrics instance. Can be changed using `set_metrics()` method."""

        self._shutdown = False

    def __bool__(self) -> bool:
        """
        Return True if pool is healthy and has available connection.
        """
        return self.queue.qsize() > 0

    @property
    def remote_app(self) -> str:
        """
        This property should be overridden in derived classes to provide more specific remote_app specification.
        """
        return "generic-connection-pool"

    def set_metrics(
        self,
        *,
        idle_connections: Optional[Gauge] = None,
        used_connections: Optional[Gauge] = None,
        connection_limit: Optional[Gauge] = None,
    ) -> None:
        """
        Set metrics to be observed. Metrics are updated on each worker thread run.
        :param idle_connections: Number of idle connections
        :param used_connections: Number of used connections
        :param connection_limit: Total number of connections this pool can manage.
        """
        self.metrics.idle_connections = idle_connections
        self.metrics.used_connections = used_connections
        self.metrics.connection_limit = connection_limit

    def _start_keeper_task(self) -> None:
        """
        Start the keeper task that checks connections and maintains pool size.
        """
        self._keeper_task_done = self.loop.create_task(self._keeper_task())
        if self._keeper_task_done:
            self._keeper_task_done.set_name("connection_pool_keeper")

    async def check_conn(self, conn: CountedConnection[ConnectionTypeT]) -> bool:  # pylint: disable=unused-argument, no-self-use
        """
        Check connection is healthy and ready to be used.
        :param conn: Connection to be checked.
        :return: True if conneciton is healthy, False if it should be discarded.
        """
        return True

    async def get_conn(self, stacklevel: int = 1) -> CountedConnection[ConnectionTypeT]:
        """
        Get connection from connection pool. If there is no connection available at the moment, it waits until
        one is available before returning.

        :param stacklevel: Number of stack frames to skip when logging.
        :return: Wrapper wrapping actual connection. All calls to methods are forwarded to actual connection and all
        properties are forwarded too.
        """
        if self.queue.empty():
            if self._connections_in_use >= self.max_pool_size:
                self.log.warning(
                    "Connection pool is empty, unable to retrieve connection immediately.", stacklevel=stacklevel + 1
                )
                conn = await self.queue.get()
            else:
                # Try to create connectiom immediately. If it fails, wait for queue to get anything from keeper.
                try:
                    conn = await self.make_conn()

                # pylint: disable=broad-except
                except Exception as exc:
                    self.log.exception(exc, stacklevel=stacklevel + 1)
                    conn = await self.queue.get()
        else:
            conn = await self.queue.get()

        # Handling of connection returning should check that there is no pending transaction on the connection and if there
        # is, don't put it back in pool. So this assert is OK, it is really an internal library error if current_transaction
        # is not None.

        # fmt: off
        assert conn.current_transaction is None, \
            "Connection still has transaction outstanding. This is probably bug in library."  # noqa: S101
        # fmt: on

        self._connections_in_use += 1
        conn.used += 1

        self.log.debug("Reusing existing connection %r.", conn)
        return conn

    async def create_conn(self) -> ConnectionTypeT:
        """
        Create new connection. This by default just calls connection constructor with args and kwargs passed to
        constructor.
        :return: Newly created connection.
        """
        return self.connection_class(*self.args, **self.kwargs)

    async def make_conn(self, use: bool = False) -> CountedConnection[ConnectionTypeT]:
        """
        Method is called internally by connection pool to create new connection. This in fact just calls create_conn()
        and wraps the connection into CountedConnection class.
        :param use: if True, the connection will be marked as used right away.
        :return: Wrapped fresh connection.
        :raises: UnableToCreateConnection if connection failed to create.
        """
        try:
            conn: CountedConnection[ConnectionTypeT] = CountedConnection(await self.create_conn())
            if not await self.check_conn(conn):
                raise UnableToCreateConnection()
        except Exception as exc:
            raise UnableToCreateConnection() from exc

        if use:
            self._connections_in_use += 1
            conn.used += 1

        self.log.debug("Created new connection %r.", conn)

        return conn

    async def return_conn(self, conn: CountedConnection[ConnectionTypeT]) -> None:
        """
        Return connection to pool, or discard it if its lifetime is exceeded.
        :param conn: Connection to be returned.
        """
        should_return = True

        try:
            if self._shutdown:
                self.log.debug("Discarding connection %r, because pool is shutting down.", conn)
                should_return = False

            if conn.current_transaction is not None:
                self.log.error(
                    "Discarding connection %r, because it still contains active transaction %r.", conn, conn.current_transaction
                )
                should_return = False

            if self._keeper_task_done is not None and self._keeper_task_done.done():
                self.log.debug("Discarding connection %r, because keeper task has finished.")
                should_return = False

            # Check whether the connection can be returned to queue.
            conn_is_over_usage = self.max_conn_usage and 0 < self.max_conn_usage <= conn.used
            conn_is_too_old = self.max_conn_lifetime and 0 < self.max_conn_lifetime <= time() - conn.created

            if not conn_is_over_usage and not conn_is_too_old and should_return:
                # Return connection to pool
                await self.queue.put(conn)
                self.log.debug("Returned connection %r to pool.", conn)
            else:
                if conn_is_over_usage:
                    self.log.debug("Discarding connection %r because it's max usage count has been depleted.", conn)
                elif conn_is_too_old:
                    self.log.debug("Discarding connection %r because it's max lifetime has been reached.", conn)

                await self.close_conn(conn)
        finally:
            self._connections_in_use -= 1

    async def close_conn(self, conn: CountedConnection[ConnectionTypeT]) -> None:  # pylint: disable=no-self-use
        """Close one connection."""
        if asyncio.iscoroutinefunction(conn.close):
            await conn.close()
        else:
            conn.close()

    async def close_connections(self) -> None:
        """
        Close all connections in the pool.
        """
        while not self.queue.empty():
            try:
                conn = self.queue.get_nowait()
                await self.close_conn(conn)
            except QueueEmpty:
                pass

    async def _shutdown_keeper_task(self) -> None:
        """
        Method used to signal the keeper task to stop. It waits until the task is finished.
        """
        self._shutdown = True
        if self._keeper_task_done is not None:
            self._keeper_task_done.cancel()

            try:
                await self._keeper_task_done
            except CancelledError:
                pass

    async def _check_conns(self) -> None:
        if self.queue.qsize() > 0 and self.max_conn_lifetime is not None and self.max_conn_lifetime > 0:
            conn = await self.queue.get()
            if time() - conn.created >= self.max_conn_lifetime:
                self.log.debug("Discarding connection %r because it's max lifetime has been reached.", conn)
                await self.close_conn(conn)
            else:
                close = False
                try:
                    if await self.check_conn(conn):
                        await self.queue.put(conn)
                    else:
                        close = True
                except Exception as exc:  # pylint: disable=broad-except
                    self.log.exception(exc)
                    close = True

                if close:
                    self.log.info("Closing unhealthy connection %r.", conn)
                    await self.close_conn(conn)

    async def _purge_conns(self, to_purge: int) -> None:
        self.log.debug("Purging %d inactive connections from pool.", to_purge)
        for _ in range(0, to_purge):
            conn = await self.queue.get()
            await self.close_conn(conn)

    async def _create_conns(self, to_create: int) -> None:
        self.log.debug("Creating %d spare connections...", to_create)
        for _ in range(0, to_create):
            try:
                conn = await self.make_conn()
                await self.queue.put(conn)
            except UnableToCreateConnection:
                # self.log.exception(exc)
                pass

            except Exception as exc:  # pylint: disable=broad-except
                self.log.exception(exc)

    def _update_metrics(self) -> None:
        """
        Update metrics based on pool sizes.
        """
        if self.metrics.idle_connections is not None:
            self.metrics.idle_connections.set(self.queue.qsize())

        if self.metrics.used_connections is not None:
            self.metrics.used_connections.set(self._connections_in_use)

        if self.metrics.connection_limit is not None:
            self.metrics.connection_limit.set(self.max_pool_size)

    async def _keeper_task_iteration(self) -> None:
        """
        One iteration of keeper task.
        """

        # pylint: disable=try-except-raise
        try:
            self._update_metrics()
            await self._check_conns()

            to_purge = min(self.queue.qsize() - self.max_spare_conns, self.queue.qsize() - self.min_spare_conns)
            to_create = min(
                self.min_spare_conns - self.queue.qsize(), self.max_pool_size - self.queue.qsize() - self._connections_in_use
            )

            if to_purge > 0:
                await self._purge_conns(to_purge)

            elif to_create > 0:
                await self._create_conns(to_create)

        except CancelledError:
            raise

        except Exception as exc:  # pylint: disable=broad-except
            self.log.exception(exc)

    async def _keeper_task(self) -> None:
        """
        Keeper task maintaining lifecycle of the connections. It periodically checks connections for health,
        discards too old connections and spins new ones when there is not enough connections in the pool.
        It is started automatically for each pool as it is created, and terminated when connection pool is closed.
        """
        try:
            while True:
                await self._keeper_task_iteration()
                await asyncio.sleep(1.0)
        except CancelledError:
            pass

    @property
    def current_pool_size(self) -> int:
        """
        Return total number of connections currently handled by pool.
        """
        return self.queue.qsize() + self._connections_in_use

    @property
    def current_pool_free(self) -> int:
        """
        Return number of currently available connections for instant use.
        """
        return self.queue.qsize()

    @property
    def connections_in_use(self) -> int:
        """Number of connections currently in use."""
        return self._connections_in_use

    async def close(self) -> None:
        """
        Close the pool. Stop the keeper task and do cleanup.
        """
        self.log.debug("Shutting down pool.")
        await self._shutdown_keeper_task()
        await self.close_connections()
        self.log.debug("Pool shutdown complete.")


TransactionTypeT = TypeVar("TransactionTypeT", bound=Transaction[Any])


class DatabaseConnectionPool(Generic[ConnectionTypeT, TransactionTypeT], ConnectionPool[ConnectionTypeT]):
    """
    Connection pool for database connections.
    """

    _pool_cache: ClassVar[dict[tuple[str, str], DatabaseConnectionPool[Any, Any]]] = {}

    @classmethod
    def create(cls, *args: Any, **kwargs: Any) -> DatabaseConnectionPool[ConnectionTypeT, TransactionTypeT]:
        """
        Create new connection pool.
        :param args: Arguments passed to the connection pool constructor.
        :param kwargs: Keyword arguments passed to the connection pool constructor.
        :return: Created connection pool.
        """
        key = (str(args), str(kwargs))

        if key not in cls._pool_cache:
            cls._pool_cache[key] = cls(*args, **kwargs)

        return cls._pool_cache[key]

    def __init__(self, transaction_class: Type[TransactionTypeT], *args: Any, **kwargs: Any) -> None:
        """
        :param transaction_class: Class that is returned when transaction is requested.
        :param args: Arguments to the super class constructor.
        :param kwargs: Keyword arguments to the super class constructor.
        """
        super().__init__(*args, **kwargs)
        self.transaction_class = transaction_class
        """Class used to create transaction instances."""

    async def get_transaction(self, *, stacklevel: int = 1) -> TransactionTypeT:
        """
        Get transaction from the connection. Use this primarily instead of get_conn(), as transaction is the
        access class that you should work with.
        :param stacklevel: Stack level to skip when logging.
        :return: Instance of transaction class.
        """
        conn = await self.get_conn(stacklevel + 1)
        trans = PooledTransaction(self.transaction_class(conn), pool=self)
        conn.current_transaction = trans

        # PooledTransaction is not really a TransactionTypeT, but it is object wrapper that acts like it is. For better
        # type hinting in IDE, we cast this to expected type. In runtime, it just works by proxying all calls to the
        # actual transaction object.
        return cast(TransactionTypeT, trans)


T = TypeVar("T", bound=ConnectionPool[Any])


class ConnectionPoolSingleton(Generic[T]):
    """
    Just a basic singleton for connection pool.
    """

    def __init__(self, cls: Type[T], *args: Any, **kwargs: Any):
        """
        :param cls: Connection pool class
        :param args: Arguments to the connection pool constructor.
        :param kwargs: Keyword arguments to the connection pool constructor.
        """
        self.cls: Type[T] = cls
        self.args = args
        self.kwargs = kwargs
        self._instance: Optional[T] = None

    def instance(self) -> T:
        """
        Return instance of the connection pool. If there is none so far, it is created.
        :return: Connection pool instance.
        """
        if self._instance is None:
            self._instance = self.cls(*self.args, **self.kwargs)

        return self._instance


@asynccontextmanager
async def transaction(
    pool: DatabaseConnectionPool[ConnectionTypeT, Transaction[ConnectionTypeT]],
) -> AsyncGenerator[Transaction[ConnectionTypeT], None]:
    """
    Return database transaction from the pool as async context.
    """
    async with await pool.get_transaction() as trans:
        yield trans
