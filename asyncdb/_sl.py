from contextvars import ContextVar
from types import TracebackType
from typing import Optional, Self, Type


class _SL:
    """
    Stack level context variable, to provide correct, context-aware logging messages from outside the aiomysql
    scope (to log actual query execution from user's code, not from the library).
    """

    sl: ContextVar[int] = ContextVar("_SL.sl", default=0)

    @classmethod
    def get(cls) -> int:
        """Get current stack level"""
        return cls.sl.get()

    @classmethod
    def inc(cls, amount: int = 1) -> None:
        """Increase stack level"""
        cls.sl.set(cls.sl.get() + amount)

    @classmethod
    def dec(cls, amount: int = 1) -> None:
        """Decrease stack level"""
        cls.sl.set(cls.sl.get() - amount)

    def __init__(self, inc: int = 1) -> None:
        """
        :param inc: Initial stack level increment.
        """
        self._inc = inc

    def __enter__(self) -> Self:
        """
        Increase stack level on entering context.
        """
        _SL.inc(self._inc)
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """
        Decrease stack level on exiting context.
        """
        _SL.dec(self._inc)
