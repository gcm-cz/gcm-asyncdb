from types import TracebackType
from typing import Any, Awaitable, Generic, Optional, Type, TypeVar

from ._sl import _SL
from .asyncdb import Transaction

T = TypeVar("T", bound=Transaction[Any])


class TransactionContext(Generic[T]):
    """
    @private

    Transaction context that helps count stack levels correctly. This is a class that is returned from the transaction factory.

    Usage:

    >>> from asyncdb.aiomysql import TransactionFactory
    >>> db: TransactionFactory = ...
    >>> async with db.transaction() as trx:
    >>>    ...
    """

    def __init__(self, awaitable: Awaitable[T]) -> None:
        """
        :param awaitable: Awaitable that returns transaction instance to be started.
        """
        self.awaitable = awaitable
        self.trx: Optional[T] = None

    async def __aenter__(self) -> T:
        """Start the transaction on async context entry."""
        self.trx = await self.awaitable
        with _SL():
            return await self.trx.__aenter__()

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """Exits the transaction on async context exit."""
        if self.trx is not None:
            await self.trx.__aexit__(exc_type, exc_val, exc_tb)
