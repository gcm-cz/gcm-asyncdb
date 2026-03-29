from __future__ import annotations

from collections.abc import Awaitable
from contextvars import ContextVar, Token
from types import TracebackType
from typing import Generic, cast

from asyncdb._sl import _SL
from asyncdb.asyncdb.shared_transaction import SharedTransaction
from asyncdb.generics import TransactionTypeT
from asyncdb.types import ObjectProxy


class TransactionContext(Generic[TransactionTypeT]):
    """
    @private

    Transaction context that helps count stack levels correctly. This is a class that is returned from the transaction factory.

    Usage:

    >>> from asyncdb.aiomysql import TransactionFactory
    >>> db: TransactionFactory = ...
    >>> async with db.transaction() as trx:
    >>>    ...
    """

    def __init__(self, awaitable: Awaitable[TransactionTypeT | ObjectProxy[TransactionTypeT]]) -> None:
        """
        :param awaitable: Awaitable that returns transaction instance to be started.
        """
        self.awaitable = awaitable
        self.trx: TransactionTypeT | None = None

    async def __aenter__(self) -> TransactionTypeT:
        """Start the transaction on async context entry."""
        self.trx = cast(TransactionTypeT, await self.awaitable)
        with _SL():
            return await self.trx.__aenter__()

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """Exits the transaction on async context exit."""
        if self.trx is not None:
            await self.trx.__aexit__(exc_type, exc_val, exc_tb)


class ExistingTransactionContext(TransactionContext[TransactionTypeT]):
    """
    @private

    Transaction context that reuses an existing transaction. **The transaction is NOT committed/rolled back on exit from this
    context.** It is expected that it is managed from within upper level context manager (possibly ContextualTransactionContext)
    that will handle the correct termination of transaction.

    Do not use this class directly, it is there for ContextualTransactionFactory's internal use.
    """

    def __init__(self, trx: SharedTransaction[TransactionTypeT]) -> None:
        """
        :param trx: Existing transaction that should be used within this transaction context.
        """

        super().__init__(self._get_existing_transaction_helper())
        self.trx = cast(TransactionTypeT, trx)

    async def _get_existing_transaction_helper(self) -> TransactionTypeT:
        return self.trx


class SharedTransactionContext(Generic[TransactionTypeT], TransactionContext[TransactionTypeT]):
    """
    Transaction context manager that sets the transaction in the provided ContextVar.
    """

    def __init__(
        self,
        transaction_context: ContextVar[TransactionTypeT | None],
        transaction: Awaitable[TransactionTypeT],
    ) -> None:
        self.transaction_context = transaction_context
        self.token: Token[TransactionTypeT | None] | None = None
        super().__init__(transaction)

    async def __aenter__(self) -> TransactionTypeT:
        with _SL():
            trx = await super().__aenter__()
        self.token = self.transaction_context.set(trx)
        return trx

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        self.transaction_context.reset(self.token)
        with _SL():
            await super().__aexit__(exc_type, exc_value, traceback)
