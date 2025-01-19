from types import TracebackType
from typing import Any, Awaitable, Generic, Optional, Type, TypeVar

from ._sl import _SL
from .asyncdb import Transaction

T = TypeVar("T", bound=Transaction[Any])


class TransactionContext(Generic[T]):
    """
    Transaction context that helps count stack levels correctly.
    """

    def __init__(self, awaitable: Awaitable[T]) -> None:
        self.awaitable = awaitable
        self.trx: Optional[T] = None

    async def __aenter__(self) -> T:
        self.trx = await self.awaitable
        with _SL(2):
            return await self.trx.__aenter__()

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        if self.trx is not None:
            with _SL():
                await self.trx.__aexit__(exc_type, exc_val, exc_tb)
