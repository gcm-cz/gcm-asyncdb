import asyncio
from typing import Optional

import aiomysql

from .config import MySQLConfig
from .connection import AioMySQLConnection
from .observer import ObserverContext
from .transaction import Transaction
from .. import logger
from ..pool import DatabaseConnectionPool


class MySQLConnectionPool(DatabaseConnectionPool[AioMySQLConnection, Transaction]):
    """
    Connection pool providing aiomysql transactions.
    """

    def __init__(self, config: Optional[MySQLConfig] = None, **kwargs):
        """
        Initialize the connection pool
        :param config: MySQLConfig with configuration.
        :param kwargs: Any additional arguments for either the connection pool or the database constructor.
        """
        super().__init__(Transaction, AioMySQLConnection, max_pool_size=config.max_pool_size,
                         max_spare_conns=config.max_spare_conns, min_spare_conns=config.min_spare_conns,
                         max_conn_lifetime=config.max_conn_lifetime, max_conn_usage=config.max_conn_usage, **kwargs)
        self.config = config or MySQLConfig()

    @property
    def remote_app(self) -> str:
        return f"{self.config.user}@{self.config.host}:{self.config.port}/{self.config.database}"

    async def create_conn(self) -> AioMySQLConnection:
        """
        Create new connection.
        :return: Created connection.
        """
        conn = self.connection_class(host=self.config.host, port=self.config.port, user=self.config.user,
                                     password=self.config.password, db=self.config.database,
                                     charset=self.config.charset, autocommit=False, use_unicode=True,
                                     connect_timeout=self.config.connect_timeout,
                                     init_command="SET time_zone='+00:00'; SET net_read_timeout={}; SET net_write_timeout={};".format(
                                         self.config.read_timeout, self.config.write_timeout
                                     ))

        conn.logger.debug("Connecting to mysql://%s@%s:%d/%s", self.config.user, self.config.host, self.config.port, self.config.database)

        with ObserverContext(logger=logger.getChild("mysql.startup")):
            try:
                await asyncio.wait_for(conn._connect(), self.config.connect_timeout)
                conn.logger.debug("Connected!")
                return conn
            except (aiomysql.OperationalError, asyncio.TimeoutError) as exc:
                if hasattr(exc, "__cause__") and getattr(exc, "__cause__"):
                    conn.logger.error(
                        "Connection failed with %s (caused by %s)",
                        str(exc) or exc.__class__.__name__,
                        str(exc.__cause__) or exc.__cause__.__class__.__name__
                    )
                else:
                    conn.logger.error("Connection failed with %s", str(exc) or exc.__class__.__name__)
                raise
            except Exception as exc:
                conn.logger.exception(exc)
                raise

    async def check_conn(self, conn: AioMySQLConnection) -> bool:
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
            health_logger.error(f"{exc.__class__.__qualname__}: {str(exc)}")
            return False

    async def close_conn(self, conn: AioMySQLConnection):
        self.log.debug("Closing connection %r.", conn)
        await conn.ensure_closed()
