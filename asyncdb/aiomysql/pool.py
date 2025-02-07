import asyncio
from collections.abc import Collection
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional, Type

import aiomysql

from .. import logger
from .._sl import _SL
from ..pool import CountedConnection, DatabaseConnectionPool
from .config import MySQLConfig, MySQLConfigProtocol
from .connection import AioMySQLConnection
from .observer import ObserverContext, QueryObserver, TransactionObserver
from .transaction import Transaction, TransactionIsolationLevel


class MySQLConnectionPool(DatabaseConnectionPool[AioMySQLConnection, Transaction]):
    """
    Connection pool providing aiomysql transactions.
    """

    def __init__(
        self,
        config: Optional[MySQLConfigProtocol] = None,
        connection_class: Type[AioMySQLConnection] = AioMySQLConnection,
        connection_observers: Optional[Collection[QueryObserver]] = None,
        transaction_observers: Optional[Collection[TransactionObserver]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the connection pool
        :param config: MySQLConfig with configuration.
        :param connection_class: Connection class to use for the pool. Defaults to `AioMySQLConnection`.
        :param connection_observers: Collection of observers to attach to each connection.
        :param transaction_observers: Collection of observers to attach to each transaction.
        :param kwargs: Any additional arguments for either the connection pool or the database constructor.
        """
        self.config = config or MySQLConfig()
        """Configuration used to spawn MySQL connections and to configure the pool."""

        self._connection_observers = connection_observers
        self._transaction_observers = transaction_observers

        super().__init__(
            Transaction,
            connection_class,
            max_pool_size=self.config.max_pool_size,
            max_spare_conns=self.config.max_spare_conns,
            min_spare_conns=self.config.min_spare_conns,
            max_conn_lifetime=self.config.max_conn_lifetime,
            max_conn_usage=self.config.max_conn_usage,
            **kwargs,
        )

        self._health_logger = self.log.getChild("health")

    @property
    def remote_app(self) -> str:
        """Remote application identifier for the connections in the pool."""
        if not hasattr(self.config, "remote_app") or self.config.remote_app is None:
            return f"{self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}"

        return self.config.remote_app

    async def create_conn(self) -> AioMySQLConnection:
        """
        Create new connection.
        :return: Created connection.
        """
        with _SL(3) as sl:
            try:
                kwargs: dict[str, Any] = {}

                if hasattr(self.config, "timezone"):
                    kwargs["timezone"] = self.config.timezone

                if hasattr(self.config, "read_timeout"):
                    kwargs["read_timeout"] = self.config.read_timeout

                if hasattr(self.config, "write_timeout"):
                    kwargs["write_timeout"] = self.config.write_timeout

                if hasattr(self.config, "wait_timeout"):
                    kwargs["wait_timeout"] = self.config.wait_timeout

                conn: AioMySQLConnection = self.connection_class(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    db=self.config.database,
                    charset=self.config.charset,
                    autocommit=False,
                    use_unicode=True,
                    connect_timeout=self.config.connect_timeout,
                    remote_app=self.remote_app,
                    **kwargs,
                )

                for observer in self._connection_observers or []:
                    conn.attach(observer)

                conn.logger.debug(
                    "Connecting to mysql://%s@%s:%d/%s",
                    self.config.user,
                    self.config.host,
                    self.config.port,
                    self.config.database,
                    stacklevel=sl.get(),
                )
            except Exception as exc:
                self.log.exception("Unable to create connection: %r", exc)
                raise

            with ObserverContext(logger=logger.getChild("mysql.startup")):
                try:
                    # pylint: disable=protected-access
                    await asyncio.wait_for(conn._connect(), self.config.connect_timeout)
                    conn.logger.debug("Connected!", stacklevel=sl.get())
                    return conn
                except (aiomysql.OperationalError, asyncio.TimeoutError) as exc:
                    if hasattr(exc, "__cause__") and exc.__cause__:
                        conn.logger.error(
                            "Connection failed with %s (caused by %s)",
                            str(exc) or exc.__class__.__name__,
                            str(exc.__cause__) or exc.__cause__.__class__.__name__,
                            stacklevel=sl.get(),
                        )
                    else:
                        conn.logger.error("Connection failed with %s", str(exc) or exc.__class__.__name__, stacklevel=sl.get())
                    raise
                except Exception as exc:
                    conn.logger.exception(exc, stacklevel=sl.get())
                    raise

    async def check_conn(self, conn: CountedConnection[AioMySQLConnection]) -> bool:
        """
        Check whether given connection is healthy. Returning True means the connection can be used for next
        transactions, False it should be discarded.
        :param conn: Connection to check.
        :return: True if connection is healthy and ready to be used for next transaction, False if it should be
          discarded.
        """
        try:
            await conn.query("SELECT 1", logger=self._health_logger)
            return True

        # pylint: disable=broad-except  # Because that is exactly what we want to do.
        except Exception as exc:
            self._health_logger.error("%s: %s", exc.__class__.__qualname__, exc)
            return False

    async def close_conn(self, conn: AioMySQLConnection) -> None:
        """
        Close MySQL connection.
        :param conn: Connection to close.
        """
        self.log.debug("Closing connection %r.", conn)
        await conn.ensure_closed()

    async def get_transaction(  # type: ignore[override]
        self,
        isolation_level: Optional[TransactionIsolationLevel] = None,
        *,
        stacklevel: int = 1,
    ) -> Transaction:
        # pylint: disable=arguments-differ
        """
        Get transaction from the connection. Use this primarily instead of get_conn(), as transaction is the
        access class that you should work with.
        :param isolation_level: Transaction isolation level.
        :param stacklevel: Stack level to skip when logging.
        :return: Instance of transaction class.
        """
        return await super().get_transaction(
            isolation_level=isolation_level, stacklevel=stacklevel + 1, observers=self._transaction_observers
        )


@asynccontextmanager
async def transaction(
    pool: MySQLConnectionPool,
    isolation_level: Optional[TransactionIsolationLevel] = None,
    timeout: Optional[float] = None,  # noqa: ASYNC109
) -> AsyncGenerator[Transaction, None]:
    """
    Return database transaction from the pool as async context.
    """
    async with await asyncio.wait_for(
        pool.get_transaction(isolation_level=isolation_level), timeout or pool.config.connect_timeout
    ) as trans:
        yield trans
