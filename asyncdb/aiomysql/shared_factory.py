from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Collection
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Unpack, cast
from warnings import warn

from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT

from .._sl import _SL
from ..context import T, TransactionContext
from ..exceptions import ImplicitRollback, IsolationLevelMismatch, OutsideTransactionContext
from ..types import ObjectProxy
from .config import MySQLConfigProtocol
from .connection import AioMySQLConnection
from .factory import TransactionFactory, TransactionKwargs
from .observer import QueryObserver, TransactionObserver
from .transaction import SharedRollbackMode, Transaction, TransactionIsolationLevel


class SharedTransaction(ObjectProxy[Transaction]):
    """
    @private

    A shared transaction proxy that allows sharing the same transaction instance across different contexts.
    This is useful for reusing an existing transaction without committing or rolling it back in the current context.
    """

    def __init__(self, wrapped: Transaction, propagate_rollback: bool = True) -> None:
        super().__init__(wrapped)
        self._self_commit: bool = False
        self._propagate_rollback = propagate_rollback

    async def commit(self) -> None:
        """
        Commit of the shared transaction does nothing.
        """
        self._self_commit = True

    async def rollback(self) -> None:
        """
        Rollback of the shared transaction does nothing.
        """
        self._self_commit = True
        if self._propagate_rollback:
            self.__wrapped__.__shared_can_commit__ = False  # pylint: disable=no-member

    async def __aenter__(self) -> T:
        """
        On enter, simply returns the wrapped transaction.
        """
        return cast(T, self)

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        On exit, if the transaction was not committed or rolled back, it warns about implicit rollback.
        """
        if not self._self_commit:
            warn(
                "Exiting shared transaction without committing or rolling back. "
                "This may lead to implicit rollback when upper level transaction is not available.",
                ImplicitRollback,
                stacklevel=_SL.get() + 2,
            )


class ExistingTransactionContext(TransactionContext[T]):
    """
    @private

    Transaction context that reuses an existing transaction. **The transaction is NOT committed/rolled back on exit from this
    context.** It is expected that it is managed from within upper level context manager (possibly ContextualTransactionContext)
    that will handle the correct termination of transaction.

    Do not use this class directly, it is there for ContextualTransactionFactory's internal use.
    """

    def __init__(self, trx: SharedTransaction) -> None:
        """
        :param trx: Existing transaction that should be used within this transaction context.
        """

        super().__init__(self._get_existing_transaction_helper())
        self.trx = cast(T, trx)

    async def _get_existing_transaction_helper(self) -> T:
        return self.trx


class SharedTransactionContext(TransactionContext[Transaction]):
    """
    Transaction context manager that sets the transaction in the provided ContextVar.
    """

    def __init__(
        self,
        transaction_context: ContextVar[Transaction | None],
        transaction: Awaitable[Transaction],
    ) -> None:
        self.transaction_context = transaction_context
        self.token: Token[Transaction | None] | None = None
        super().__init__(transaction)

    async def __aenter__(self) -> Transaction:
        with _SL():
            trx = await super().__aenter__()
            self.token = self.transaction_context.set(trx)
            return trx

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        with _SL():
            self.transaction_context.reset(self.token)
            await super().__aexit__(exc_type, exc_value, traceback)


class SharedTransactionFactory(TransactionFactory):
    """
    Extension to TransactionFactory, that automatically re-uses the active transaction within nested calls acquiring transaction.

    This is mainly usefull when you are using abstract classes to define interface for managing entities, regardless of whether
    the entity is stored in DB or for example on some remote API. You can have one signature for both DB and remote API
    implementations, and the DB implementation will acquire shared transaction created from within the upper scope.

    Example:

    >>> from asyncdb.aiomysql import MySQLConfig, TransactionFactory, Transaction
    >>> from fastapi import FastAPI, Depends
    >>> from typing import Any
    >>>
    >>> config: MySQLConfigProtocol = MySQLConfig(...)
    >>> db = SharedTransactionFactory(config)
    >>> app = FastAPI()
    >>>
    >>> async def internal_get_products() -> list[dict[str, Any]]:
    >>>     # Re-uses transaction from external context. If called outside of transaction context, raises
    >>>     # OutsideTransactionContext error.
    >>>     async with db.transaction(shared=True) as trx:
    >>>         q = await trx.query("SELECT * FROM `products`")
    >>>         out = [row async for row in q.fetch_all_dict()]
    >>>         await trx.commit()
    >>>     return out
    >>>
    >>> @app.get("/products")
    >>> async def get_products(trx: Transaction = Depends(db)):
    >>>     # No need to pass trx here.
    >>>     out = await internal_get_products()  # Think of this call as some complex structure in object hierarchy.
    >>>     await trx.commit()
    >>>     return out
    """

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
        propagate_rollback: bool = True,
        shared_rollback_mode: SharedRollbackMode = SharedRollbackMode.WARN,
    ) -> None:
        """
        :param config: Configuration of the MySQL pool.
        :param isolation_level: Transaction isolation level for transactions created using this factory. If None,
            server's default isolation level is used.
        :param connection_class: Connection class to use for the pool. Defaults to `AioMySQLConnection`.
        :param connection_observers: Optional collection of connection observers to attach to each connection in the pool.
        :param transaction_observers: Optional collection of transaction observers to attach to each transaction created
        :param retry_stop: Stop strategy for tenacity retrying.
        :param retry_wait: Wait strategy for tenacity retrying.
        :param retry_retry: Retry condition for tenacity
        :param propagate_rollback: Whether rollback called on shared transaction should propagate to the upper level transaction.
        :param shared_rollback_mode: Mode of handling propagated rollback from shared transactions.
        """
        # pylint: disable=too-many-arguments

        super().__init__(
            config,
            isolation_level,
            connection_class,
            connection_observers,
            transaction_observers,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
        )

        self.transaction_context: ContextVar[Transaction | None] = ContextVar(
            "SharedTransactionFactory.transaction_context", default=None
        )

        self._propagate_rollback = propagate_rollback
        self._shared_rollback_mode = shared_rollback_mode

    def _shared_transaction(
        self, isolation_level: TransactionIsolationLevel | None = None, propagate_rollback: bool | None = None
    ) -> TransactionContext[Transaction]:
        existing_trx = self.transaction_context.get()

        if existing_trx is None:
            raise OutsideTransactionContext(
                "Called shared_transaction() outside of transaction context. There is no active transaction to re-use."
            )

        if existing_trx.isolation_level != (isolation_level or self.isolation_level):
            raise IsolationLevelMismatch(
                "Cannot reuse existing transaction with different isolation level "
                f"(existing: {existing_trx.isolation_level}, requested: {isolation_level or self.isolation_level})"
            )

        return ExistingTransactionContext(
            SharedTransaction(existing_trx, propagate_rollback if propagate_rollback is not None else self._propagate_rollback)
        )

    def transaction(
        self,
        isolation_level: TransactionIsolationLevel | None = None,
        timeout: float | None = None,
        *,
        shared: bool = False,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        propagate_rollback: bool | None = None,
        **kwargs: Unpack[TransactionKwargs],
    ) -> TransactionContext[Transaction]:
        """
        Same as `TransactionFactory.transaction()`, but also stores the active transaction in context variable for further
        usage by `shared_transaction()` call.

        Example:

        >>> from asyncdb.aiomysql import MySQLConfigProtocol, MySQLConfig, SharedTransactionFactory
        >>>
        >>> config: MySQLConfigProtocol = MySQLConfig(...)
        >>> db = SharedTransactionFactory(config)
        >>>
        >>> async def internal_get_products() -> list[dict]:
        >>>     # Re-uses transaction from external context. If called outside of transaction context, raises
        >>>     # OutsideTransactionContext error.
        >>>     async with db.transaction(shared=True) as trx:
        >>>         q = await trx.query("SELECT * FROM `products`")
        >>>         out = [row async for row in q.fetch_all_dict()]
        >>>
        >>>         # No commit or rollback here, that's handled by outer transaction.
        >>>     return out
        >>>
        >>> async def get_products():
        >>>     async with db.transaction() as trx:
        >>>         # Receives trx as shared transaction.
        >>>         await internal_get_products()  # Think of it as complex structure in object hierarchy.
        >>>         await trx.commit()

        :param shared: If True, tries to reuse existing transaction from upper context. If no transaction exists in the context,
            a new transaction is created.
        :param propagate_rollback: Whether rollback called on shared transaction should propagate to the upper level transaction.
            If None, uses the factory's default.
        :param shared_rollback_mode: Mode of handling propagated rollback from shared transactions. If None, uses the
            factory's default.

        :raises IsolationLevelMismatch: when requesting shared transaction and there is an active transaction with different
            isolation level than requested.
        """
        if shared is True and self.transaction_context.get() is not None:
            with _SL():
                return self._shared_transaction(isolation_level, propagate_rollback)

        if kwargs.get("shared_rollback_mode") is None:
            kwargs["shared_rollback_mode"] = self._shared_rollback_mode

        return SharedTransactionContext(
            self.transaction_context,
            self._get_transaction_from_pool(
                isolation_level or self.isolation_level,
                timeout or self.config.connect_timeout,
                retry_stop=retry_stop or self._retry_stop,
                retry_wait=retry_wait or self._retry_wait,
                retry_retry=retry_retry or self._retry_retry,
                **kwargs,
            ),
        )

    async def __call__(self) -> AsyncGenerator[Transaction, None]:
        with _SL():
            async with self.transaction(shared=True) as trx:
                with _SL(-1):
                    yield trx
