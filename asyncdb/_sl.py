from contextvars import ContextVar


class _SL:
    """
    Stack level context variable, to provide correct, context-aware logging messages from outside the aiomysql
    scope (to log actual query execution from user's code, not from the library).
    """

    sl: ContextVar[int] = ContextVar("_SL.sl", default=0)

    @classmethod
    def get(cls) -> int:
        return cls.sl.get()

    @classmethod
    def inc(cls, amount: int = 1):
        cls.sl.set(cls.sl.get() + amount)

    @classmethod
    def dec(cls, amount: int = 1):
        cls.sl.set(cls.sl.get() - amount)

    def __init__(self, inc: int = 1):
        self._inc = inc

    def __enter__(self):
        _SL.inc(self._inc)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _SL.dec(self._inc)
