from __future__ import annotations

from collections.abc import Collection
from typing import Any, Generic

from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT
from typing_extensions import override

from asyncdb.aiomysql.config import DEFAULT_RETRY, DEFAULT_RETRY_STOP, DEFAULT_RETRY_WAIT, MySQLConfigProtocol
from asyncdb.aiomysql.connection import AioMySQLConnection
from asyncdb.aiomysql.factory import AioMySQLConnectionTypeT, BaseMySQLTransactionFactory
from asyncdb.aiomysql.pool import MySQLConnectionPool
from asyncdb.aiomysql.transaction import SharedRollbackMode, Transaction, TransactionIsolationLevel
from asyncdb.asyncdb.observer import GenericQueryObserver, GenericTransactionObserver
from asyncdb.context import TransactionContext
from asyncdb.exceptions import IsolationLevelMismatch
from asyncdb.shared_factory import SharedTransactionFactory as BaseSharedTransactionFactory


class SharedTransactionFactory(
    Generic[AioMySQLConnectionTypeT],
    BaseMySQLTransactionFactory[AioMySQLConnectionTypeT],
    BaseSharedTransactionFactory[MySQLConnectionPool, AioMySQLConnectionTypeT, Transaction],
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
        config: MySQLConfigProtocol,
        isolation_level: TransactionIsolationLevel | None = None,
        connection_class: type[AioMySQLConnectionTypeT] = AioMySQLConnection,
        connection_observers: Collection[GenericQueryObserver[Any, Any]] | None = None,
        transaction_observers: (Collection[GenericTransactionObserver[Any, Any, Any, Any]] | None) = None,
        *,
        retry_stop: StopBaseT = DEFAULT_RETRY_STOP,
        retry_wait: WaitBaseT = DEFAULT_RETRY_WAIT,
        retry_retry: RetryBaseT = DEFAULT_RETRY,
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
            propagate_rollback=propagate_rollback,
            shared_rollback_mode=shared_rollback_mode,
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
        **kwargs: Any,
    ) -> TransactionContext[Transaction]:
        return super().transaction(
            isolation_level=isolation_level,
            timeout=timeout,
            shared=shared,
            retry_stop=retry_stop,
            retry_wait=retry_wait,
            retry_retry=retry_retry,
            propagate_rollback=propagate_rollback,
            **kwargs,
        )

    @override
    def _shared_transaction(
        self, *, propagate_rollback: bool | None = None, isolation_level: TransactionIsolationLevel | None = None
    ) -> TransactionContext[Transaction]:
        existing_trx = self.transaction_context.get()

        if existing_trx and existing_trx.isolation_level != (isolation_level or self.isolation_level):
            raise IsolationLevelMismatch(
                "Cannot reuse existing transaction with different isolation level "
                f"(existing: {existing_trx.isolation_level}, requested: {isolation_level or self.isolation_level})"
            )

        return super()._shared_transaction(propagate_rollback=propagate_rollback)
