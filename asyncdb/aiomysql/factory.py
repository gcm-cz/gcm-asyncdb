import asyncio
from collections.abc import AsyncGenerator, Collection, Hashable
from contextlib import _AsyncGeneratorContextManager
from typing import Any, Generic

from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT
from typing_extensions import TypeVar

from asyncdb.aiomysql.config import DEFAULT_RETRY, DEFAULT_RETRY_STOP, DEFAULT_RETRY_WAIT, MySQLConfigProtocol
from asyncdb.aiomysql.connection import AioMySQLConnection
from asyncdb.aiomysql.pool import MySQLConnectionPool
from asyncdb.aiomysql.transaction import Transaction, TransactionIsolationLevel
from asyncdb.asyncdb.observer import GenericQueryObserver, GenericTransactionObserver
from asyncdb.context import TransactionContext
from asyncdb.factory import TransactionFactory as BaseTransactionFactory

AioMySQLConnectionTypeT = TypeVar("AioMySQLConnectionTypeT", bound=AioMySQLConnection, default=AioMySQLConnection)


class BaseMySQLTransactionFactory(
    Generic[AioMySQLConnectionTypeT], BaseTransactionFactory[MySQLConnectionPool, AioMySQLConnectionTypeT, Transaction]
):
    """
    Base class providing MySQL connection handling.
    """

    def __init__(
        self,
        config: MySQLConfigProtocol,
        isolation_level: TransactionIsolationLevel | None = None,
        connection_class: type[AioMySQLConnectionTypeT] = AioMySQLConnection,
        connection_observers: Collection[GenericQueryObserver[Any, Any]] | None = None,
        transaction_observers: Collection[GenericTransactionObserver[Any, Any, Any, Any]] | None = None,
        *,
        retry_stop: StopBaseT = DEFAULT_RETRY_STOP,
        retry_wait: WaitBaseT = DEFAULT_RETRY_WAIT,
        retry_retry: RetryBaseT = DEFAULT_RETRY,
        **kwargs: Any,
    ) -> None:
        # pylint: disable=too-many-arguments
        """
        :param config: Configuration of the MySQL pool.
        :param isolation_level: Default transaction isolation level for transactions created using this factory. If None,
            server's default isolation level is used.
        :param connection_class: Connection class to use for the pool. Defaults to `AioMySQLConnection`.
        :param connection_observers: Optional collection of connection observers to attach to each connection in the pool.
        :param transaction_observers: Optional collection of transaction observers to attach to each transaction created
            from the pool.
        :param retry_stop: Default tenacity stop strategy for deadlock retries.
            If None, default from `config.DEFAULT_RETRY_STOP` will be used.
        :param retry_wait: Default tenacity wait strategy for deadlock retries.
            If None, default from `config.DEFAULT_RETRY_WAIT` will be used.
        :param retry_retry: Default tenacity retry condition for deadlock retries, on top of default Deadlock.
        :param kwargs: Additional arguments passed to the transaction creation.
        """
        super().__init__(
            connection_observers=connection_observers,
            transaction_observers=transaction_observers,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
            **kwargs,
        )

        self._connection_class = connection_class
        self.config = config
        """Configuration of the MySQL pool."""

        self.isolation_level = isolation_level
        """Default transaction isolation level for transactions created using this factory. If None, server's default
        isolation level is used."""

    @property
    def _cache_key(self) -> Hashable:
        return self.config.host, self.config.port, self.config.user, self.config.database

    def _create_pool(self, *, keeper_task: bool = True) -> MySQLConnectionPool:
        return MySQLConnectionPool(
            self.config,
            connection_class=self._connection_class,
            connection_observers=self._connection_observers,
            transaction_observers=self._transaction_observers,
            keeper_task=keeper_task,
        )

    async def _get_transaction_from_pool(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        isolation_level: TransactionIsolationLevel | None = None,
        **kwargs: Any,
    ) -> Transaction:
        return await asyncio.wait_for(
            self.pool.get_transaction(
                isolation_level or self.isolation_level,
                retry_stop=retry_stop or self._retry_stop,
                retry_wait=retry_wait or self._retry_wait,
                retry_retry=retry_retry or self._retry_retry,
                **kwargs,
            ),
            timeout or self.config.connect_timeout,
        )


class TransactionFactory(Generic[AioMySQLConnectionTypeT], BaseMySQLTransactionFactory[AioMySQLConnectionTypeT]):
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

    def transaction(
        self,
        isolation_level: TransactionIsolationLevel | None = None,
        timeout: float | None = None,
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        **kwargs: Any,
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
        :param retry_stop: Stop strategy for tenacity retrying. If None, default from factory is used.
        :param retry_wait: Wait strategy for tenacity retrying. If None, default from factory is used.
        :param retry_retry: Retry condition for tenacity retrying, to specify additional retryable errors.
          If None, default from factory is used. Note that `Deadlock` is always retried.
        :return: Transaction context manager.
        :raises: asyncio.TimeoutError if healthy connection cannot be acquired in given timeout.
        """
        return super().transaction(
            timeout=timeout,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
            isolation_level=isolation_level,
            **kwargs,
        )

    async def retry(
        self,
        isolation_level: TransactionIsolationLevel | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        **kwargs: Any,
    ) -> AsyncGenerator[_AsyncGeneratorContextManager[Transaction], None]:
        async for attempt in super().retry(
            isolation_level=isolation_level,
            timeout=timeout,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
            **kwargs,
        ):
            yield attempt
