from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, AsyncIterator, Collection, Hashable
from contextlib import _AsyncGeneratorContextManager, asynccontextmanager
from typing import Any, ClassVar, Generic, cast

from tenacity import AsyncRetrying, AttemptManager, RetryCallState
from tenacity.retry import RetryBaseT, retry_any, retry_if_exception_type
from tenacity.stop import StopBaseT, stop_after_delay
from tenacity.wait import WaitBaseT, wait_exponential

from asyncdb import TransactionContext
from asyncdb._sl import _SL
from asyncdb.asyncdb.observer import GenericQueryObserver, GenericTransactionObserver
from asyncdb.generics import ConnectionTypeT, DatabaseConnectionPoolT, TransactionTypeT


class TransactionFactory(Generic[DatabaseConnectionPoolT, ConnectionTypeT, TransactionTypeT], ABC):
    """
    Base class for transaction factories. Implements some common patterns, the rest is abstract and left to concrete
    implementation in specific database drivers.
    """

    _pool_cache: ClassVar[dict[Hashable, Any]] = {}

    def __init__(
        self,
        *,
        connection_observers: Collection[GenericQueryObserver[Any, ConnectionTypeT]] | None = None,
        transaction_observers: Collection[GenericTransactionObserver[Any, Any, ConnectionTypeT, TransactionTypeT]] | None = None,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        **kwargs: Any,
    ) -> None:
        """
        :param retry_stop: Tenacity stop strategy for retries. If None, default is to stop after 10 seconds.
        :param retry_wait: Tenacity wait strategy for retries. If None, default is exponential backoff with multiplier 0.5,
            min 0.01s and max 1s.
        :param retry_retry: Tenacity retry strategy for retries. If None, default is to retry on any exception.
        :param kwargs: Additional keyword arguments for subclasses.
        """

        self._connection_observers = connection_observers or []
        self._transaction_observers = transaction_observers or []

        self._retry_stop = retry_stop or stop_after_delay(2)
        self._retry_wait = retry_wait or wait_exponential(multiplier=0.5, min=0.01, max=1)
        self._retry_retry = retry_retry or retry_if_exception_type(BaseException)

        super().__init__(**kwargs)

    @property
    @abstractmethod
    def _cache_key(self) -> Hashable:
        """
        Cache key for the connection pool.
        """

    @abstractmethod
    def _create_pool(self, *, keeper_task: bool = True) -> DatabaseConnectionPoolT:
        """
        Create the connection pool. This method is called when the pool is first accessed, and should create and return
        the connection pool instance.

        :param: Whether start the connection pool's keeper task, which is responsible for maintaining the pool and creating new
            connections as needed. This is useful when you want to control the pool's lifecycle manually, for example in tests.
        :return: Connection pool instance.
        """

    @abstractmethod
    async def _get_transaction_from_pool(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        **kwargs: Any,
    ) -> TransactionTypeT:
        """
        Get transaction from the connection pool.

        :param timeout: Timeout for waiting for available transaction from the pool.
        :param retry_stop: Tenacity stop strategy for retries. If None, default from factory configuration will be used.
        :param retry_wait: Tenacity wait strategy for retries. If None, default from factory configuration will be used.
        :param retry_retry: Tenacity retry strategy for retries. If None, default from factory configuration will be used.
        :param kwargs: Additional keyword arguments passed to the transaction creation.
        :return: Transaction instance.
        """

    def ensure_pool(self, *, keeper_task: bool = True) -> None:
        """
        Ensure the pool exists and connections are being created by keeper task. The pool is created when first used, this
        forces the creation of pool beforehand, to be ready when first connection is requested.

        Pools are shared by all factories with the same configuration. When the shared pool has been closed (for example
        by the shutdown of another application instance in the same process), a new pool is created in its place.

        :param keeper_task: When set to False, the pool's keeper task is not started. This is useful when you want to control
            the pool's lifecycle manually, for example in tests.
        """
        cached = self.__class__._pool_cache.get(self._cache_key)  # pylint: disable=protected-access
        if cached is None or cached.closed:
            self.__class__._pool_cache[self._cache_key] = self._create_pool(keeper_task=keeper_task)  # pylint: disable=protected-access

    @property
    def pool(self) -> DatabaseConnectionPoolT:
        """
        Get the connection pool, ensuring it is initialized.

        Returns:
            Connection pool instance.
        """
        self.ensure_pool()
        return self._pool_cache[self._cache_key]

    async def __call__(self) -> AsyncGenerator[TransactionTypeT]:
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
        async with await self._get_transaction_from_pool() as trx:
            yield trx

    def transaction(
        self,
        *,
        timeout: float | None = None,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        **kwargs: Any,
    ) -> TransactionContext[TransactionTypeT]:
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
        return TransactionContext[TransactionTypeT](
            # No await here, as it is awaited when entering the context.
            self._get_transaction_from_pool(
                timeout=timeout,
                retry_stop=retry_stop or self._retry_stop,
                retry_wait=retry_wait or self._retry_wait,
                retry_retry=retry_retry or self._retry_retry,
                **kwargs,
            )
        )

    async def retry(
        self,
        *,
        timeout: float | None = None,  # noqa: ASYNC109
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        stacklevel: int = 0,
        **kwargs: Any,
    ) -> AsyncGenerator[_AsyncGeneratorContextManager[TransactionTypeT], None]:
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
        :param kwargs: Optional additional keyword arguments to pass to the transaction call.
        :return:
        """

        @asynccontextmanager
        async def trx_context(trx: TransactionTypeT, attempt: AttemptManager) -> AsyncIterator[TransactionTypeT]:
            with attempt:
                yield trx

            if attempt.retry_state.outcome.failed:
                attempt.retry_state.kwargs["trx"] = trx

        async def _observe_retry(retry_state: RetryCallState) -> None:
            with _SL(2):
                await cast(TransactionTypeT, retry_state.kwargs["trx"])._observe_retry(attempt.retry_state)  # pylint: disable=protected-access

        if not retry_retry:
            retry_retry = self._retry_retry

        async for attempt in AsyncRetrying(
            retry=retry_any(self._retry_retry, retry_retry) if retry_retry else self._retry_retry,  # type: ignore[arg-type]
            wait=retry_wait or self._retry_wait,
            stop=retry_stop or self._retry_stop,
            before_sleep=_observe_retry,
            reraise=True,
        ):
            with _SL(2 + stacklevel):
                async with self.transaction(timeout=timeout, **kwargs) as trx:
                    with _SL(-2):
                        yield trx_context(trx, attempt)
