import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Collection
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from typing import ClassVar, cast

from tenacity import AsyncRetrying, AttemptManager, RetryCallState, retry_any
from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT

from .._sl import _SL
from ..context import TransactionContext
from .config import DEFAULT_RETRY, DEFAULT_RETRY_STOP, DEFAULT_RETRY_WAIT, MySQLConfigProtocol
from .connection import AioMySQLConnection
from .observer import QueryObserver, TransactionObserver
from .pool import MySQLConnectionPool
from .transaction import Transaction, TransactionIsolationLevel


class TransactionFactory:
    # pylint: disable=too-many-instance-attributes
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
        isolation_level: TransactionIsolationLevel | None = None,
        connection_class: type[AioMySQLConnection] = AioMySQLConnection,
        connection_observers: Collection[QueryObserver] | None = None,
        transaction_observers: Collection[TransactionObserver] | None = None,
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
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

        self._retry_stop = retry_stop
        self._retry_wait = retry_wait
        self._retry_retry = retry_retry

    def ensure_pool(self, *, keeper_task: bool = True) -> None:
        # pylint: disable=protected-access
        """
        Ensure the pool exists and connections are being created by keeper task. The pool is created when first used, this
        forces the creation of pool beforehand, to be ready when first connection is requested.

        :param keeper_task: When set to False, the pool's keeper task is not started. This is useful when you want to control
            the pool's lifecycle manually, for example in tests.
        """

        if self._cache_key not in self.__class__._pool_cache:
            self.__class__._pool_cache[self._cache_key] = MySQLConnectionPool(
                self.config,
                connection_class=self._connection_class,
                connection_observers=self._connection_observers,
                transaction_observers=self._transaction_observers,
                keeper_task=keeper_task,
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
        regarding timeout and transaction isolation level. This is mainly usefull as FastAPI dependency, as it does
        not generate any additional dependencies, that might get propagated all the way to the end user's documentation.

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
        """
        async with await asyncio.wait_for(
            self.pool.get_transaction(
                self.isolation_level,
                retry_stop=self._retry_stop,
                retry_wait=self._retry_wait,
            ),
            self.config.connect_timeout,
        ) as trx:
            yield trx

    def transaction(
        self,
        isolation_level: TransactionIsolationLevel | None = None,
        timeout: float | None = None,
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
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
        return TransactionContext[Transaction](
            asyncio.wait_for(
                self.pool.get_transaction(
                    isolation_level or self.isolation_level,
                    retry_stop=retry_stop or self._retry_stop,
                    retry_wait=retry_wait or self._retry_wait,
                    retry_retry=retry_retry or self._retry_retry,
                ),
                timeout or self.config.connect_timeout,
            )
        )

    async def retry(
        self,
        isolation_level: TransactionIsolationLevel | None = None,
        timeout: float | None = None,  # noqa: ASYNC109
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
    ) -> AsyncGenerator[_AsyncGeneratorContextManager[Transaction], None]:
        """
        Generator version of the Transaction's deadlock retry decorator. The difference is, that decorator uses the same
        transaction which is reset each time, and needs to be used on a function. Meanwhile this generator can be used inside
        a code to wrap only part of the code.

        Example:

        >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, TransactionFactory
        >>>
        >>> config: MySQLConfigProtocol = MySQLConfig(...)
        >>> db = TransactionFactory(config)
        >>>
        >>> async def transfer_funds(from_account: int, to_account: int, amount: float):
        >>>     async for attempt in db.retry():
        >>>         async with attempt as trx:
        >>>             from_balance = await trx.get_scalar("SELECT balance FROM accounts WHERE id = %s", [from_account])
        >>>             to_balance = await trx.get_scalar("SELECT balance FROM accounts WHERE id = %s", [to_account])
        >>>             if from_balance < amount:
        >>>                 raise Exception("Insufficient funds")
        >>>             await trx.query("UPDATE accounts SET balance = balance - %s WHERE id = %s", amount, from_account)
        >>>             await trx.query("UPDATE accounts SET balance = balance + %s WHERE id = %s", amount, to_account)
        >>>             await trx.commit()

        :param isolation_level: Transaction isolation level. If None, defaults to isolation level from factory.
        :param timeout: Timeout waiting for healthy connection. Defaults to connect_timeout from pool configuration.
        :param retry_stop: Stop strategy for tenacity retrying. If None, default from factory is used.
        :param retry_wait: Wait strategy for tenacity retrying. If None, default from factory is used.
        :param retry_retry: Retry condition for tenacity retrying, to specify additional retryable errors.
          If None, default from factory is used. Note that `Deadlock` is always retried.
        :return:
        """

        @asynccontextmanager
        async def trx_context(trx: Transaction, attempt: AttemptManager) -> AsyncIterator[Transaction]:
            with _SL(-1):
                with attempt:
                    yield trx

                if attempt.retry_state.outcome.failed:
                    attempt.retry_state.kwargs["trx"] = trx

        async def _observe_retry(retry_state: RetryCallState) -> None:
            await cast(Transaction, retry_state.kwargs["trx"])._observe_retry(attempt.retry_state)  # pylint: disable=protected-access

        if not retry_retry:
            retry_retry = self._retry_retry

        with _SL(1):
            async for attempt in AsyncRetrying(
                retry=retry_any(DEFAULT_RETRY, retry_retry) if retry_retry else DEFAULT_RETRY,  # type: ignore[arg-type]
                wait=retry_wait or self._retry_wait or DEFAULT_RETRY_WAIT,
                stop=retry_stop or self._retry_stop or DEFAULT_RETRY_STOP,
                before_sleep=_observe_retry,
                reraise=True,
            ):
                async with self.transaction(isolation_level=isolation_level, timeout=timeout) as trx:
                    yield trx_context(trx, attempt)
