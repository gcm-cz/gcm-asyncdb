import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Optional

import aiomysql

from .. import logger
from ..pool import CountedConnection, DatabaseConnectionPool, PooledTransaction
from .config import MySQLConfig, MySQLConfigProtocol
from .connection import AioMySQLConnection
from .observer import ObserverContext
from .transaction import Transaction, TransactionIsolationLevel


class MySQLConnectionPool(DatabaseConnectionPool[AioMySQLConnection, Transaction]):
    """
    Connection pool providing aiomysql transactions.
    """

    def __init__(self, config: Optional[MySQLConfigProtocol] = None, **kwargs: Any) -> None:
        """
        Initialize the connection pool
        :param config: MySQLConfig with configuration.
        :param kwargs: Any additional arguments for either the connection pool or the database constructor.
        """
        self.config = config or MySQLConfig()
        super().__init__(
            Transaction,
            AioMySQLConnection,
            max_pool_size=self.config.max_pool_size,
            max_spare_conns=self.config.max_spare_conns,
            min_spare_conns=self.config.min_spare_conns,
            max_conn_lifetime=self.config.max_conn_lifetime,
            max_conn_usage=self.config.max_conn_usage,
            **kwargs,
        )

    @property
    def remote_app(self) -> str:
        if self.config.remote_app is None:
            return f"{self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}"

        return self.config.remote_app

    async def create_conn(self) -> AioMySQLConnection:
        """
        Create new connection.
        :return: Created connection.
        """
        conn = self.connection_class(
            host=self.config.host,
            port=self.config.port,
            user=self.config.user,
            password=self.config.password,
            db=self.config.database,
            charset=self.config.charset,
            autocommit=False,
            use_unicode=True,
            connect_timeout=self.config.connect_timeout,
            init_command=f"SET time_zone='+00:00'; "
            f"SET net_read_timeout={self.config.read_timeout}; "
            f"SET net_write_timeout={self.config.write_timeout};",
        )

        conn.logger.debug(
            "Connecting to mysql://%s@%s:%d/%s", self.config.user, self.config.host, self.config.port, self.config.database
        )

        with ObserverContext(logger=logger.getChild("mysql.startup")):
            try:
                # pylint: disable=protected-access
                await asyncio.wait_for(conn._connect(), self.config.connect_timeout)
                conn.logger.debug("Connected!")
                return conn
            except (aiomysql.OperationalError, asyncio.TimeoutError) as exc:
                if hasattr(exc, "__cause__") and exc.__cause__:
                    conn.logger.error(
                        "Connection failed with %s (caused by %s)",
                        str(exc) or exc.__class__.__name__,
                        str(exc.__cause__) or exc.__cause__.__class__.__name__,
                    )
                else:
                    conn.logger.error("Connection failed with %s", str(exc) or exc.__class__.__name__)
                raise
            except Exception as exc:
                conn.logger.exception(exc)
                raise

    async def check_conn(self, conn: CountedConnection[AioMySQLConnection]) -> bool:
        """
        Check whether given connection is healthy. Returning True means the connection can be used for next
        transactions, False it should be discarded.
        :param conn: Connection to check.
        :return: True if connection is healthy and ready to be used for next transaction, False if it should be
          discarded.
        """
        health_logger = self.log.getChild("health")

        try:
            await conn.query("SELECT 1", logger=health_logger)
            return True

        # pylint: disable=broad-except  # Because that is exactly what we want to do.
        except Exception as exc:
            health_logger.error("%s: %s", exc.__class__.__qualname__, exc)
            return False

    async def close_conn(self, conn: AioMySQLConnection) -> None:
        self.log.debug("Closing connection %r.", conn)
        await conn.ensure_closed()

    async def get_transaction(
        self, isolation_level: Optional[TransactionIsolationLevel] = None, *, stacklevel: int = 1
    ) -> PooledTransaction[Transaction]:
        """
        Get transaction from the connection. Use this primarily instead of get_conn(), as transaction is the
        access class that you should work with.
        :param isolation_level: Transaction isolation level.
        :param stacklevel: Stack level to skip when logging.
        :return: Instance of transaction class.
        """
        conn = await self.get_conn(stacklevel + 1)
        trans = PooledTransaction(self.transaction_class(conn, isolation_level), pool=self)
        conn.current_transaction = trans

        return trans


@asynccontextmanager
async def transaction(
    pool: MySQLConnectionPool, isolation_level: Optional[TransactionIsolationLevel]
) -> AsyncGenerator[Transaction, None]:
    """
    Return database transaction from the pool as async context.
    """
    async with await pool.get_transaction(isolation_level) as trans:
        yield trans
