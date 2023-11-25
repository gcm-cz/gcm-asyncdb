"""
Connection pool (not only) for databases.
"""
from __future__ import annotations

import asyncio
from abc import ABC
from asyncio import Queue, CancelledError, QueueEmpty
from contextlib import asynccontextmanager
from datetime import datetime
from time import time
from typing import Type, TypeVar, Generic, Dict, Tuple, Optional, TYPE_CHECKING, Any, AsyncGenerator

from . import logger
from ._sl import _SL
from .asyncdb import Transaction

if TYPE_CHECKING:
    from prometheus_client import Gauge


class CountedConnectionMixin(ABC):
    """
    Helper class that is injected to each connection to be able to track usage of the connection to provide lifecycle
    to the pool.
    """

    def __init__(self, *args, **kwargs):
        """
        :param args: Arguments passed to the actual connection constructor.
        :param kwargs: Keyword argumens passed to the actual connection constructor.
        """
        super().__init__(*args, **kwargs)

        self.used = 0
        self.created = time()
        self.current_transaction = None

    def close(self):
        """
        Close the connection. This is here to provide predictable interface, as connection class might not
        provide close() method.
        """
        if hasattr(super(), "close"):
            return getattr(super(), "close")()

        return None


class PolledTransactionMixin(ABC):
    """
    Helper class that is injected to each transaction to return transaction to the pool when not used anymore.
    """

    def __init__(self, *args, pool: 'ConnectionPool', **kwargs):
        """
        :param args: Arguments to the actual transaction
        :param pool: Connection pool this transaction has been spinned from
        :param kwargs: Keyword arguments to the actual transaction.
        """
        super().__init__(*args, **kwargs)
        self.pool = pool

    async def __aexit__(self, *args, **kwargs) -> None:
        """
        Return connection to the pool when the transaction's context is left.
        :param args: Args passed to transaction's __aexit__ (if the is one).
        :param kwargs: Keyword arguments passed to transaction's __aexit__ (if there is one).
        """
        try:
            if hasattr(super(), "__aexit__"):
                with _SL():
                    await getattr(super(), "__aexit__")(*args, **kwargs)
        finally:
            await self.pool.return_conn(self.connection)   # type: ignore[attr-defined]


class UnableToCreateConnection(RuntimeError):
    """
    Exception thrown when connection cannot be created. It might be caused by connection failint to establish or
    initial check after establishing the connection fails.
    """


C = TypeVar('C')
C2 = TypeVar('C2')  # pylint: disable=invalid-name


class PoolMetrics:
    idle_connections: Optional[Gauge] = None
    used_connections: Optional[Gauge] = None
    connection_limit: Optional[Gauge] = None


# pylint: disable=too-many-instance-attributes
class ConnectionPool(Generic[C]):
    """
    Generic connection pool for anything.
    """

    class WrappedConnection(CountedConnectionMixin, Generic[C2]):
        """
        Wrapper for connection incorporating CountedConnectionMixin to handle lifecycle of the pool.
        """

        def __init__(self, conn: C2):
            """
            :param conn: Actual connection that is being wrapped.
            """
            super().__init__()
            self.conn = conn

        def __getattr__(self, item):
            """
            Proxy attribute access to the actual connection.
            """
            return getattr(self.conn, item)

        def __repr__(self) -> str:
            """
            Add used and created fields to the actual connection representation.
            """
            return f"{self.conn!r} used={self.used}, created={datetime.fromtimestamp(self.created)}"

    def __init__(self, connection_class: Type[C], *args,
                 max_pool_size: int = 100, max_spare_conns: int = 10, min_spare_conns: int = 5,
                 max_conn_lifetime: int = 300, max_conn_usage: int = 100, keeper_task: bool = True, **kwargs):
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
        if max_spare_conns > max_pool_size:
            max_spare_conns = max_pool_size

        if min_spare_conns > max_spare_conns:
            min_spare_conns = max_spare_conns

        if max_conn_lifetime < 0:
            raise AttributeError("max_conn_lifetime must be greater or equal to zero")

        if max_conn_usage < 0:
            raise AttributeError("max_conn_usage must be greater or equal to zero")

        self.connection_class = connection_class
        self.log = logger.getChild("pool")
        self.loop = kwargs.get("loop", asyncio.get_running_loop())

        self.args = args
        self.kwargs = kwargs

        self.max_pool_size = max_pool_size
        self.max_spare_conns = max_spare_conns
        self.min_spare_conns = min_spare_conns
        self.max_conn_lifetime = max_conn_lifetime
        self.max_conn_usage = max_conn_usage

        self.queue = Queue[ConnectionPool.WrappedConnection[C]]()

        self.connections_in_use = 0

        self._keeper_task_done = None
        if keeper_task:
            self.start_keeper_task()

        self.metrics = PoolMetrics()

        self._shutdown = False

    def __bool__(self) -> bool:
        """
        Return True if pool is healthy and have available connection.
        """
        return self.queue.qsize() > 0

    @property
    def remote_app(self) -> str:
        """
        This property should be overriden in derived classes to provide more specific remote_app specification.
        """
        return "generic-connection-pool"

    def set_metrics(self, *, idle_connections: Gauge = None, used_connections: Gauge = None, connection_limit: Gauge = None) -> None:
        """
        Set metrics to be observed. Metrics are updated on each worker thread run.
        :param idle_connections: Number of idle connections
        :param used_connections: Number of used connections
        :param connection_limit: Total number of connections this pool can manage.
        """
        self.metrics.idle_connections = idle_connections
        self.metrics.used_connections = used_connections
        self.metrics.connection_limit = connection_limit

    def start_keeper_task(self):
        """
        Start the keeper task.
        """
        self._keeper_task_done = self.loop.create_task(self.keeper_task())
        self._keeper_task_done.set_name("connection_pool_keeper")

    # pylint: disable=unused-argument  # As it is needed for actual implementation in derived classes.
    async def check_conn(self, conn: WrappedConnection[C]) -> bool:
        """
        Check connection is healthy and ready to be used.
        :param conn: Connection to be checked.
        :return: True if conneciton is healthy, False if it should be discarded.
        """
        return True

    async def get_conn(self, stacklevel: int = 1) -> WrappedConnection[C]:
        """
        Get connection from connection pool. If there is no connection available at the moment, it waits until
        one is available before returning.

        :param stacklevel: Number of stack frames to skip when logging.
        :return: Wrapper wrapping actual connection. All calls to methods are forwarded to actual connection and all
        properties are forwarded too.
        """
        if self.queue.empty():
            if self.connections_in_use >= self.max_pool_size:
                self.log.warning(
                    "Connection pool is empty, unable to retrieve connection immediately.",
                    stacklevel=stacklevel + 1
                )
                conn = await self.queue.get()
            else:
                # Try to create connectiom immediately. If it fails, wait for queue to get anything from keeper.
                try:
                    conn = await self.make_conn()
                except Exception as exc:
                    self.log.exception(exc, stacklevel=stacklevel + 1)
                    conn = await self.queue.get()
        else:
            conn = await self.queue.get()

        self.connections_in_use += 1
        conn.used += 1

        assert conn.current_transaction is None

        self.log.debug("Reusing existing connection %r.", conn)
        return conn

    async def create_conn(self) -> C:
        """
        Create new connection. This by default just calls connection constructor with args and kwargs passed to
        constructor.
        :return: Newly created connection.
        """
        return self.connection_class(*self.args, **self.kwargs)

    async def make_conn(self, use: bool = False) -> WrappedConnection[C]:  # noqa: D401
        """
        Method is called internally by connection pool to create new connection. This in fact just calls create_conn()
        and wraps the connection into WrappedConnection class.
        :param use: if True, the connection will be marked as used right away.
        :return: Wrapped fresh connection.
        :raises: UnableToCreateConnection if connection failed to create.
        """
        try:
            conn = self.WrappedConnection(await self.create_conn())
            if not await self.check_conn(conn):
                raise UnableToCreateConnection()
        except Exception as exc:
            raise UnableToCreateConnection() from exc

        if use:
            self.connections_in_use += 1
            conn.used += 1

        self.log.debug("Created new connection %r.", conn)

        return conn

    async def return_conn(self, conn: WrappedConnection[C]) -> None:
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
                self.log.error("Discarding connection %r, because it still contains active transaction %r.", conn, conn.current_transaction)
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

                await self.close_conn(conn.conn)
        finally:
            self.connections_in_use -= 1

    async def close_conn(self, conn: C):
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
                await self.close_conn(conn.conn)
            except QueueEmpty:
                pass

    async def shutdown_keeper_task(self) -> None:  # noqa: D401
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
                await self.close_conn(conn.conn)
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
                    await self.close_conn(conn.conn)

    async def _purge_conns(self, to_purge: int):
        self.log.debug("Purging %d inactive connections from pool.", to_purge)
        for _ in range(0, to_purge):
            conn = await self.queue.get()
            await self.close_conn(conn.conn)

    async def _create_conns(self, to_create: int):
        self.log.debug("Creating %d spare connections...", to_create)
        for _ in range(0, to_create):
            try:
                conn = await self.make_conn()
                await self.queue.put(conn)
            except UnableToCreateConnection as exc:
                #self.log.exception(exc)
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
            self.metrics.used_connections.set(self.connections_in_use)

        if self.metrics.connection_limit is not None:
            self.metrics.connection_limit.set(self.max_pool_size)

    async def keeper_task_iteration(self) -> None:
        """
        One iteration of keeper task.
        """
        try:
            self._update_metrics()
            await self._check_conns()

            to_purge = min(self.queue.qsize() - self.max_spare_conns, self.queue.qsize() - self.min_spare_conns)
            to_create = min(self.min_spare_conns - self.queue.qsize(),
                            self.max_pool_size - self.queue.qsize() - self.connections_in_use)

            if to_purge > 0:
                await self._purge_conns(to_purge)

            elif to_create > 0:
                await self._create_conns(to_create)

        # pylint: disable=try-except-raise   # Because we need to propagate CancelledError outside the while
        # loop, and not catch it by following block.
        except CancelledError:
            raise

        except Exception as exc:  # pylint: disable=broad-except
            self.log.exception(exc)

    async def keeper_task(self) -> None:
        """
        Keeper task maintaining lifecycle of the connections. It periodically checks connections for health,
        discards too old connections and spins new ones when there is not enough connections in the pool.
        It is started automatically for each pool as it is created, and terminated when connection pool is closed.
        """
        try:
            while True:
                await self.keeper_task_iteration()
                await asyncio.sleep(1.0)
        except CancelledError:
            pass

    @property
    def current_pool_size(self) -> int:
        """
        Return total number of connections currently handled by pool.
        """
        return self.queue.qsize() + self.connections_in_use

    @property
    def current_pool_free(self) -> int:
        """
        Return number of currently available connections for instant use.
        """
        return self.queue.qsize()

    async def close(self) -> None:
        """
        Close the pool. Stop the keeper task and do cleanup.
        """
        self.log.debug("Shutting down pool.")
        await self.shutdown_keeper_task()
        await self.close_connections()
        self.log.debug("Pool shutdown complete.")


# pylint: disable=invalid-name
TR = TypeVar('TR', bound=Transaction)


class DatabaseConnectionPool(Generic[C, TR], ConnectionPool[C]):
    """
    Connection pool for database connections.
    """

    _pool_cache: Dict[Tuple[str, str], ConnectionPool[C]] = {}

    @classmethod
    def create(cls, *args, **kwargs):
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

    def __init__(self, transaction_class: Type[TR], *args, **kwargs):
        """
        :param transaction_class: Class that is returned when transaction is requested.
        :param args: Arguments to the super class constructor.
        :param kwargs: Keyword arguments to the super class constructor.
        """
        super().__init__(*args, **kwargs)

        class WrappedTransaction(PolledTransactionMixin, transaction_class):  # type: ignore[valid-type,misc]
            """
            Class inheriting transaction_class and incorporating PolledTransactionMixin, that is used as
            actual transaction returned by get_transaction() method.
            """

        self.transaction_class = WrappedTransaction

    async def get_transaction(self, isolation_level: Optional[Any] = None, stacklevel: int = 1) -> TR:
        """
        Get transaction from the connection. Use this primarily instead of get_conn(), as transaction is the
        access class that you should work with.
        :param isolation_level: Isolation level for the transaction.
        :param stacklevel: Stack level to skip when logging.
        :return: Instance of transaction class.
        """
        conn = await self.get_conn(stacklevel + 1)
        trans = self.transaction_class(conn, isolation_level, pool=self)
        conn.current_transaction = trans

        return trans


T = TypeVar('T', bound=ConnectionPool)


class ConnectionPoolSingleton(Generic[T]):
    """
    Just a basic singleton for connection pool.
    """

    def __init__(self, cls: Type[T], *args, **kwargs):
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
async def transaction(pool: DatabaseConnectionPool, isolation_level: Optional[Any] = None) -> AsyncGenerator[Transaction, None]:
    """
    Return database transaction from the pool as async context.
    """
    async with await pool.get_transaction(isolation_level) as trans:
        yield trans
