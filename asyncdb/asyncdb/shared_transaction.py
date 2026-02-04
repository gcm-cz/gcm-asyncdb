from types import TracebackType
from typing import Generic, cast
from warnings import warn

from asyncdb._sl import _SL
from asyncdb.exceptions import ImplicitRollback
from asyncdb.generics import TransactionTypeT
from asyncdb.types import ObjectProxy


class SharedTransaction(Generic[TransactionTypeT], ObjectProxy[TransactionTypeT]):
    """
    @private

    A shared transaction proxy that allows sharing the same transaction instance across different contexts.
    This is useful for reusing an existing transaction without committing or rolling it back in the current context.
    """

    def __init__(self, wrapped: TransactionTypeT, propagate_rollback: bool = True) -> None:
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

    async def __aenter__(self) -> TransactionTypeT:
        """
        On enter, simply returns the wrapped transaction.
        """
        return cast(TransactionTypeT, self)

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
