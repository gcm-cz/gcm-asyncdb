import asyncio
from typing import Optional, AsyncGenerator

from .config import MySQLConfigProtocol
from .pool import MySQLConnectionPool
from .transaction import TransactionIsolationLevel, Transaction
from ..context import TransactionContext


class TransactionFactory:
    """
    Transaction factory from given MySQL connection pool. Can be used as FastAPI dependency, or directly by invoking
    TransactionFactory.transaction().
    """
    _pool_cache: dict[tuple[str, int, str, str], MySQLConnectionPool] = {}

    def __init__(self, config: MySQLConfigProtocol, isolation_level: Optional[TransactionIsolationLevel] = None):
        self.key = (config.host, config.port, config.user, config.database)
        self.config = config
        self.isolation_level = isolation_level

    def ensure_pool(self):
        if self.key not in self.__class__._pool_cache:
            self.__class__._pool_cache[self.key] = MySQLConnectionPool(self.config)

    @property
    def pool(self):
        self.ensure_pool()
        return self.__class__._pool_cache[self.key]

    async def __call__(self) -> AsyncGenerator[Transaction, None]:
        async with await asyncio.wait_for(self.pool.get_transaction(self.isolation_level), self.config.connect_timeout) as trx:
            yield trx

    def transaction(self, isolation_level: TransactionIsolationLevel = None, timeout: float = None) -> TransactionContext[Transaction]:
        return TransactionContext[Transaction](asyncio.wait_for(self.pool.get_transaction(
            isolation_level or self.isolation_level,
        ), timeout or self.config.connect_timeout))
