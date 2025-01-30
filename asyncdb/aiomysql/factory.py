import asyncio
from collections.abc import Collection
from typing import AsyncGenerator, ClassVar, Optional, Type

from ..context import TransactionContext
from .config import MySQLConfigProtocol
from .connection import AioMySQLConnection
from .observer import QueryObserver, TransactionObserver
from .pool import MySQLConnectionPool
from .transaction import Transaction, TransactionIsolationLevel


class TransactionFactory:
    """
    Transaction factory from given MySQL connection pool. Can be used as FastAPI dependency, or directly by invoking
    `TransactionFactory.transaction()`.

    You can instantiate multiple factories with the same configuration and they will share the same connection pool.
    This is a design choice. If you want to have separate pools, you can inherit this class and override the `pool` property.

    Example:

    With FastAPI:

    >>> from asyncdb.aiomysql import MySQLConfig, TransactionFactory, Transaction
    >>> from fastapi import FastAPI, Depends
    >>>
    >>> config: MySQLConfigProtocol = MySQLConfig(...)
    >>> db = TransactionFactory(config)
    >>> app = FastAPI()
    >>>
    >>> @app.get("/products")
    >>> async def get_products(trx: Transaction = Depends(db)):
    >>>     q = await trx.query("SELECT * FROM `products`")
    >>>     out = [row async for row in q.fetch_all_dict()]
    >>>     await trx.commit()
    >>>     return out

    Standalone:

    >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, TransactionFactory
    >>>
    >>> config: MySQLConfigProtocol = MySQLConfig(...)
    >>> db = TransactionFactory(config)
    >>>
    >>> async def get_products():
    >>>     async with db.transaction() as trx:
    >>>         q = await trx.query("SELECT * FROM `products`")
    >>>         out = [row async for row in q.fetch_all_dict()]
    >>>         await trx.commit()
    >>>    return out

    Specifying transaction isolation level:

    >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, TransactionFactory, TransactionIsolationLevel
    >>>
    >>> config: MySQLConfigProtocol = MySQLConfig(...)
    >>> db = TransactionFactory(config)
    >>>
    >>> async def get_products():
    >>>     async with db.transaction(isolation_level=TransactionIsolationLevel.READ_COMMITTED) as trx:
    >>>         # Transaction is open with READ_COMMITTED isolation level.
    >>>
    >>>         q = await trx.query("SELECT * FROM `products`")
    >>>         out = [row async for row in q.fetch_all_dict()]
    >>>         await trx.commit()
    >>>
    >>>    return out

    Or as implicit setting:

    >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, TransactionFactory, TransactionIsolationLevel
    >>>
    >>> config: MySQLConfigProtocol = MySQLConfig(...)
    >>> db = TransactionFactory(config, isolation_level=TransactionIsolationLevel.READ_COMMITTED)
    >>>
    >>> async def get_products():
    >>>     async with db.transaction() as trx:
    >>>         # Transaction is open with READ_COMMITTED isolation level.
    >>>
    >>>         q = await trx.query("SELECT * FROM `products`")
    >>>         out = [row async for row in q.fetch_all_dict()]
    >>>         await trx.commit()
    >>>
    >>>     return out
    >>>
    >>> async def create_products():
    >>>     async with db.transaction(isolation_level=TransactionIsolationLevel.READ_UNCOMMITTED) as trx:
    >>>         # Transaction is open with READ_UNCOMMITTED isolation level, overwrites default setting from TransactionFactory.
    >>>
    >>>         await trx.query("INSERT INTO `products` (`name`) VALUES ('New Product')")
    >>>         out = await trx.get_scalar("SELECT COUNT(*) FROM `products`")
    >>>         await trx.commit()
    >>>
    >>>     return out
    """

    _pool_cache: ClassVar[dict[tuple[str, int, str, str], MySQLConnectionPool]] = {}

    def __init__(
        self,
        config: MySQLConfigProtocol,
        isolation_level: Optional[TransactionIsolationLevel] = None,
        connection_class: Type[AioMySQLConnection] = AioMySQLConnection,
        connection_observers: Optional[Collection[QueryObserver]] = None,
        transaction_observers: Optional[Collection[TransactionObserver]] = None,
    ) -> None:
        """
        :param config: Configuration of the MySQL pool.
        :param isolation_level: Default transaction isolation level for transactions created using this factory. If None,
            server's default isolation level is used.
        :param connection_class: Connection class to use for the pool. Defaults to `AioMySQLConnection`.
        """
        self._cache_key = (config.host, config.port, config.user, config.database)

        self.config = config
        """Configuration of the MySQL pool."""

        self.isolation_level = isolation_level
        """Default transaction isolation level for transactions created using this factory. If None, server's default
        isolation level is used."""

        self._connection_class = connection_class
        self._connection_observers = connection_observers or []
        self._transaction_observers = transaction_observers or []

    def ensure_pool(self) -> None:
        # pylint: disable=protected-access
        """
        Ensure the pool exists and connections are being created by keeper task. The pool is created when first used, this
        forces the creation of pool beforehand, to be ready when first connection is requested.
        """

        if self._cache_key not in self.__class__._pool_cache:
            self.__class__._pool_cache[self._cache_key] = MySQLConnectionPool(
                self.config,
                connection_class=self._connection_class,
                connection_observers=self._connection_observers,
                transaction_observers=self._transaction_observers,
            )

    @property
    def pool(self) -> MySQLConnectionPool:
        """
        Returns MySQL pool instance that this factory uses to get connections from.
        """
        # pylint: disable=protected-access

        self.ensure_pool()
        return self.__class__._pool_cache[self._cache_key]

    async def __call__(self) -> AsyncGenerator[Transaction, None]:
        """
        Yields transaction instance from one of the pool connections. Uses default configuration from the factory
        regarding timeout and transaction isolation level.

        Example:

        >>> from asyncdb.aiomysql import MySQLConfig, TransactionFactory, Transaction
        >>> from fastapi import FastAPI, Depends
        >>>
        >>> config: MySQLConfigProtocol = MySQLConfig(...)
        >>> db = TransactionFactory(config)
        >>> app = FastAPI()
        >>>
        >>> @app.get("/products")
        >>> async def get_products(trx: Transaction = Depends(db)):
        >>>     q = await trx.query("SELECT * FROM `products`")
        >>>     out = [row async for row in q.fetch_all_dict()]
        >>>     await trx.commit()
        >>>     return out

        :return:
        """
        async with await asyncio.wait_for(self.pool.get_transaction(self.isolation_level), self.config.connect_timeout) as trx:
            yield trx

    def transaction(
        self, isolation_level: Optional[TransactionIsolationLevel] = None, timeout: Optional[float] = None
    ) -> TransactionContext[Transaction]:
        """
        Returns transaction with optionally specifying isolation level and / or timeout for the connection.

        Example:

        >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, TransactionFactory
        >>>
        >>> config: MySQLConfigProtocol = MySQLConfig(...)
        >>> db = TransactionFactory(config)
        >>>
        >>> async def get_products():
        >>>     async with db.transaction() as trx:
        >>>         q = await trx.query("SELECT * FROM `products`")
        >>>         out = [row async for row in q.fetch_all_dict()]
        >>>         await trx.commit()
        >>>     return out

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
