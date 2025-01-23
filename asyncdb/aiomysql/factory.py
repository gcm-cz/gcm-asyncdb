import asyncio
from typing import AsyncGenerator, ClassVar, Optional

from ..context import TransactionContext
from .config import MySQLConfigProtocol
from .pool import MySQLConnectionPool
from .transaction import Transaction, TransactionIsolationLevel


class TransactionFactory:
    """
    Transaction factory from given MySQL connection pool. Can be used as FastAPI dependency, or directly by invoking
    TransactionFactory.transaction().
    """

    _pool_cache: ClassVar[dict[tuple[str, int, str, str], MySQLConnectionPool]] = {}

    def __init__(self, config: MySQLConfigProtocol, isolation_level: Optional[TransactionIsolationLevel] = None) -> None:
        """
        :param config: Configuration of the MySQL pool.
        :param isolation_level: Default transaction isolation level for transactions created using this factory. If None,
            server's default isolation level is used.
        """
        self._cache_key = (config.host, config.port, config.user, config.database)

        self.config = config
        """Configuration of the MySQL pool."""

        self.isolation_level = isolation_level
        """Default transaction isolation level for transactions created using this factory. If None, server's default
        isolation level is used."""

    def ensure_pool(self) -> None:
        # pylint: disable=protected-access
        """
        Ensure the pool exists and connections are being created by keeper task. The pool is created when first used, this
        forces the creation of pool beforehand, to be ready when first connection is requested.
        """

        if self._cache_key not in self.__class__._pool_cache:
            self.__class__._pool_cache[self._cache_key] = MySQLConnectionPool(self.config)

    @property
    def pool(self) -> MySQLConnectionPool:
        """
        Returns MySQL pool instance.
        """
        # pylint: disable=protected-access

        self.ensure_pool()
        return self.__class__._pool_cache[self._cache_key]

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
    ) -> TransactionContext[Transaction]:
        """
        Returns transaction with optionally specifying isolation level and / or timeout for the connection.
        :param isolation_level: Transaction isolation level. If None, defaults to isolation level from factory.
        :param timeout: Timeout waiting for healthy connection. Defaults to connect_timeout from pool configuration.
        :return: Transaction context manager.
        :raises: asyncio.TimeoutError if healthy connection cannot be acquired in given timeout.
        """
        return TransactionContext[Transaction](
            asyncio.wait_for(
                self.pool.get_transaction(
                    isolation_level or self.isolation_level,
                ),
                timeout or self.config.connect_timeout,
            )
        )
