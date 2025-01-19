import asyncio
from typing import AsyncGenerator, ClassVar, Optional

from ..context import TransactionContext
from ..pool import PooledTransaction
from .config import MySQLConfigProtocol
from .connection import AioMySQLConnection
from .pool import MySQLConnectionPool
from .transaction import Transaction, TransactionIsolationLevel


class TransactionFactory:
    """
    Transaction factory from given MySQL connection pool. Can be used as FastAPI dependency, or directly by invoking
    TransactionFactory.transaction().
    """

    _pool_cache: ClassVar[dict[tuple[str, int, str, str], MySQLConnectionPool]] = {}

    def __init__(self, config: MySQLConfigProtocol, isolation_level: Optional[TransactionIsolationLevel] = None) -> None:
        self.key = (config.host, config.port, config.user, config.database)
        self.config = config
        self.isolation_level = isolation_level

    def ensure_pool(self) -> None:
        """
        Ensure the pool exists and connections are being created by keeper task.
        """
        # pylint: disable=protected-access

        if self.key not in self.__class__._pool_cache:
            self.__class__._pool_cache[self.key] = MySQLConnectionPool(self.config)

    @property
    def pool(self) -> MySQLConnectionPool:
        """
        Returns MySQL pool instance.
        :return:
        """
        # pylint: disable=protected-access

        self.ensure_pool()
        return self.__class__._pool_cache[self.key]

    async def __call__(self) -> AsyncGenerator[Transaction, None]:
        """
        Yields transaction instance from one of the pool connections. Uses default configuration from the factory
        regarding timeout and transaction isolation level.
        :return:
        """
        async with await asyncio.wait_for(self.pool.get_transaction(self.isolation_level), self.config.connect_timeout) as trx:
            yield trx

    def transaction(
        self, isolation_level: Optional[TransactionIsolationLevel] = None, timeout: Optional[float] = None
    ) -> TransactionContext[PooledTransaction[AioMySQLConnection]]:
        """
        Returns transaction with optionally specifying isolation level and / or timeout for the connection.
        :param isolation_level: Transaction isolation level
        :param timeout: Timeout waiting for healthy connection
        :return: Transaction context manager.
        """
        return TransactionContext[PooledTransaction[AioMySQLConnection]](
            asyncio.wait_for(
                self.pool.get_transaction(
                    isolation_level or self.isolation_level,
                ),
                timeout or self.config.connect_timeout,
            )
        )
