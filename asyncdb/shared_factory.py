from collections.abc import AsyncGenerator, Collection
from contextlib import _AsyncGeneratorContextManager
from contextvars import ContextVar
from typing import Any, Generic

from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT

from asyncdb import TransactionContext
from asyncdb._sl import _SL
from asyncdb.asyncdb import GenericQueryObserver, GenericTransactionObserver
from asyncdb.asyncdb.shared_transaction import SharedTransaction
from asyncdb.context import ExistingTransactionContext, SharedTransactionContext
from asyncdb.exceptions import OutsideTransactionContext
from asyncdb.factory import TransactionFactory
from asyncdb.generics import ConnectionTypeT, DatabaseConnectionPoolT, TransactionTypeT
from asyncdb.types import SharedRollbackMode


class SharedTransactionFactory(
    Generic[DatabaseConnectionPoolT, ConnectionTypeT, TransactionTypeT],
    TransactionFactory[DatabaseConnectionPoolT, ConnectionTypeT, TransactionTypeT],
):
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
        *,
        connection_observers: Collection[GenericQueryObserver[Any, ConnectionTypeT]] | None = None,
        transaction_observers: Collection[GenericTransactionObserver[Any, Any, ConnectionTypeT, TransactionTypeT]] | None = None,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        propagate_rollback: bool = True,
        shared_rollback_mode: SharedRollbackMode = SharedRollbackMode.WARN,
        **kwargs: Any,
    ):
        super().__init__(
            connection_observers=connection_observers,
            transaction_observers=transaction_observers,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
            **kwargs,
        )

        self.transaction_context: ContextVar[TransactionTypeT | None] = ContextVar(
            "SharedTransactionFactory.transaction_context", default=None
        )

        self._propagate_rollback = propagate_rollback
        self._shared_rollback_mode = shared_rollback_mode

    def _shared_transaction(self, *, propagate_rollback: bool | None = None) -> TransactionContext[TransactionTypeT]:
        existing_trx = self.transaction_context.get()

        if existing_trx is None:
            raise OutsideTransactionContext(
                "Called shared_transaction() outside of transaction context. There is no active transaction to re-use."
            )

        return ExistingTransactionContext(
            SharedTransaction(existing_trx, propagate_rollback if propagate_rollback is not None else self._propagate_rollback)
        )

    def transaction(
        self,
        *,
        timeout: float | None = None,
        shared: bool = False,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        propagate_rollback: bool | None = None,
        **kwargs: Any,
    ) -> TransactionContext[TransactionTypeT]:
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
                return self._shared_transaction(propagate_rollback=propagate_rollback)

        if kwargs.get("shared_rollback_mode") is None:
            kwargs["shared_rollback_mode"] = self._shared_rollback_mode

        return SharedTransactionContext(
            self.transaction_context,
            self._get_transaction_from_pool(
                timeout=timeout,
                retry_stop=retry_stop,
                retry_wait=retry_wait,
                retry_retry=retry_retry,
                **kwargs,
            ),
        )

    async def __call__(self) -> AsyncGenerator[TransactionTypeT, None]:
        with _SL():
            async with self.transaction(shared=True) as trx:
                with _SL(-1):
                    yield trx

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
        with _SL():
            async for attempt in super().retry(
                timeout=timeout,
                retry_stop=retry_stop,
                retry_wait=retry_wait,
                retry_retry=retry_retry,
                stacklevel=stacklevel - 1,
                **kwargs,
            ):
                yield attempt
