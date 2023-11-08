from __future__ import annotations

import logging
from abc import ABC
from contextvars import ContextVar
from time import time
from typing import Any, TYPE_CHECKING, Optional

from .._sl import _SL

if TYPE_CHECKING:
    from .connection import AioMySQLConnection
    from .transaction import Transaction


class QueryObserver(ABC):
    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:
        pass

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, **kwargs: Any):
        pass

    def observe_query_error(self, conn: AioMySQLConnection, query: str, error: Exception, context: Any, **kwargs: Any):
        pass


class TransactionObserver(ABC):
    def observe_query_before(self, conn: AioMySQLConnection, query: str, trx_context: Any, **kwargs: Any) -> Any:
        pass

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, trx_context: Any, **kwargs: Any):
        pass

    def observe_query_error(self, conn: AioMySQLConnection, query: str, error: Exception, context: Any, trx_context: Any, **kwargs: Any):
        pass

    def observe_transaction_start(self, trx: Transaction, **kwargs: Any) -> Any:
        pass

    def observe_transaction_end(self, trx: Transaction, context: Any, **kwargs: Any):
        pass

    def observe_transaction_commit(self, trx: Transaction, context: Any, **kwargs: Any):
        pass

    def observe_transaction_rollback(self, trx: Transaction, context: Any, **kwargs: Any):
        pass


class LoggingObserver(QueryObserver):
    def __init__(self, logger: logging.Logger):
        self.logger = logger

    def observe_query_before(
            self,
            conn: AioMySQLConnection,
            query: str,
            *,
            logger: logging.Logger = None,
            stacklevel: int = 0,
            **kwargs: Any
    ) -> float:
        (logger or self.logger).debug(
            "Executing %s",
            query,
            stacklevel=_SL.get() + stacklevel
        )
        return time()

    def observe_query_after(
            self,
            conn: AioMySQLConnection,
            query: str,
            start_time: float,
            *,
            logger: logging.Logger = None,
            stacklevel: int = 0,
            **kwargs: Any
    ):
        (logger or self.logger).debug(
            "Took %1.4fs",
            time() - start_time,
            stacklevel=_SL.get() + stacklevel
        )

    def observe_query_error(
            self,
            conn: AioMySQLConnection,
            query: str,
            error: Exception,
            start_time: float,
            *,
            logger: logging.Logger = None,
            stacklevel: int = 0,
            **kwargs: Any
    ):
        (logger or self.logger).debug(
            f"Took %1.4fs: %s",
            time() - start_time,
            str(error),
            stacklevel=_SL.get() + stacklevel
        )


class ObserverContext:
    ctx: ContextVar[dict[str, Any]] = ContextVar('ObserverContext.ctx', default=None)

    def __init__(self, **kwargs: Any):
        self._kwargs = kwargs
        self.token = None

    def __enter__(self):
        existing_kwargs = self.__class__.ctx.get()
        if not existing_kwargs:
            existing_kwargs = {}

        self.token = self.__class__.ctx.set({**existing_kwargs, **self._kwargs})
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.__class__.ctx.reset(self.token)
        except ValueError:
            raise exc_val
            pass

    def kwargs(self, **kwargs: Any) -> dict[str, Any]:
        existing_kwargs = self.__class__.ctx.get()
        if not existing_kwargs:
            existing_kwargs = {}

        return {**existing_kwargs, **kwargs}


class _QueryObserverContext(ObserverContext):
    query_ctx: ContextVar[_QueryObserverContext] = ContextVar('_QueryObserverContext.ctx', default=None)

    def __init__(self, conn: AioMySQLConnection, **kwargs: Any):
        super().__init__(**kwargs)
        self.conn = conn
        self.sql: Optional[str] = None
        self.query_token = self.__class__.query_ctx.set(self)
        self.observer_context = {}

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sql is not None:
            if exc_type is not None:
                self.conn.observe_query_error(self.sql, exc_val, self.observer_context, **self.kwargs())
            else:
                self.conn.observe_query_after(self.sql, self.observer_context, **self.kwargs())

        try:
            self.__class__.query_ctx.reset(self.query_token)
        except ValueError:
            pass

        super().__exit__(exc_type, exc_val, exc_tb)


class _TransactionQueryObserver(QueryObserver):
    def __init__(self, trx: Transaction):
        self.trx = trx

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:
        observer_context: dict[int, Any] = {}

        with _SL():
            for observer in self.trx.observers:
                observer_context[id(observer)] = observer.observe_query_before(
                    conn,
                    query,
                    self.trx.observer_context.get(id(observer)),
                    **kwargs
                )

            return observer_context

    def observe_query_after(self, conn: AioMySQLConnection, query: str, observer_context: Any, **kwargs: Any) -> Any:
        with _SL():
            for observer in self.trx.observers:
                observer.observe_query_after(
                    conn,
                    query,
                    observer_context.get(id(observer)),
                    self.trx.observer_context.get(id(observer)),
                    **kwargs
                )

    def observe_query_error(self, conn: AioMySQLConnection, query: str, error: Exception, observer_context: Any, **kwargs: Any) -> Any:
        with _SL():
            for observer in self.trx.observers:
                observer.observe_query_error(
                    conn,
                    query,
                    error,
                    observer_context.get(id(observer)),
                    self.trx.observer_context.get(id(observer)),
                    **kwargs
                )
