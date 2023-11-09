from typing import Awaitable, TypeVar, Type, Generic, Optional

from ._sl import _SL
from .asyncdb import Transaction

T = TypeVar("T", bound=Type[Transaction])


class TransactionContext(Generic[T]):
    """
    Transaction context that helps count stack levels correctly.
    """

    def __init__(self, awaitable: Awaitable[T]):
        self.awaitable = awaitable
        self.trx: Optional[T] = None

    async def __aenter__(self) -> T:
        self.trx = await self.awaitable
        with _SL(2):
            return await self.trx.__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.trx is not None:
            with _SL():
                await self.trx.__aexit__(exc_type, exc_val, exc_tb)
