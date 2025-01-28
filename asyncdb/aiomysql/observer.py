from __future__ import annotations

import logging
from abc import ABC
from contextvars import ContextVar, Token
from time import time
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Optional, Self, Type

from .._sl import _SL

if TYPE_CHECKING:
    from .connection import AioMySQLConnection
    from .transaction import Transaction


class QueryObserver(ABC):  # noqa: B024
    """
    Abstract class for query observer. Implement this class to observe query execution. None of the methods actually needs
    to be overwritten, but you can override any of them to hook into any part of query execution process.

    Example:

    >>> import asyncio
    >>> from asyncdb.aiomysql import QueryObserver, AioMySQLConnection
    >>>
    >>> class MyObserver(QueryObserver):
    >>>     def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs):
    >>>         print(f"Query: {query}")
    >>>
    >>>     def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, **kwargs):
    >>>         print(f"Query executed")
    >>>
    >>>     def observe_query_error(self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, **kwargs):
    >>>         print(f"Query failed with error {error}")
    >>>
    >>> connection: AioMySQLConnection = ...
    >>>
    >>> # Attach observer instance to the connection.
    >>> connection.attach(MyObserver())
    >>>
    >>> async def main():
    >>>     # When executing query, observer methods are called.
    >>>     await connection.query("SELECT 1")
    >>>
    >>>     # Output:
    >>>     # Query: SELECT 1
    >>>     # Query executed
    >>>
    >>> asyncio.run(main())
    """

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:  # noqa: B027
        """
        Called before the query is sent to server for execution.
        :param conn: MySQL connection that this query will be sent to.
        :param query: Query that will be executed, including all arguments already escaped and formatted.
        :param kwargs: Additional arguments passed to the query execution method.
        :return: Any context object that will be passed to observe_query_after and observe_query_error methods.
        """

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, **kwargs: Any) -> None:  # noqa: B027
        """
        Called after the query has been executed successfully.
        :param conn: MySQL connection that this query was executed on.
        :param query: Query that has been executed.
        :param context: Context object returned by observe_query_before method.
        :param kwargs: Additional kwargs passed to the query execution method.
        """

    def observe_query_error(  # noqa: B027
        self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, **kwargs: Any
    ) -> None:
        """
        Called when the query execution has failed.
        :param conn: MySQL connection that this query was executed on.
        :param query: Query that has been executed.
        :param error: Exception that was raised during query execution.
        :param context: Context object returned by observe_query_before method.
        :param kwargs: Additional kwargs passed to the query execution method.
        """


class TransactionObserver(ABC):  # noqa: B024
    """
    Abstract class to observing transaction.
    Implement this class to observe transaction execution. None of the methods actually needs to be overwritten, but you can
    override any of them to hook into any part of transaction execution process.

    Example:

    >>> import asyncio
    >>> from asyncdb.aiomysql import Transaction, Result, TransactionObserver, AioMySQLConnection

    >>> class MyObserver(TransactionObserver):
    >>>     def observe_transaction_start(self, transaction: Transaction, **kwargs):
    >>>         print("Transaction started")
    >>>
    >>>     def observe_transaction_commit(self, transaction: Transaction, **kwargs):
    >>>         print("Transaction committed")
    >>>
    >>>     def observe_transaction_rollback(self, transaction: Transaction, **kwargs):
    >>>         print("Transaction rolled back")
    >>>
    >>>     def observe_transaction_end(self, transaction: Transaction, **kwargs):
    >>>         print("Transaction ended")
    >>>
    >>>     def observe_query_before(self, transaction: Transaction, query: str, args: tuple[Any, ...], **kwargs):
    >>>         print(f"Query: {query} with args {args}")
    >>>
    >>>     def observe_query_after(self, transaction: Transaction, result: Result, **kwargs):
    >>>         print(f"Query executed with result {result}")
    >>>
    >>>
    >>> connection: AioMySQLConnection = ...
    >>> trx = Transaction(connection)
    >>> trx.attach(MyObserver())
    >>>
    >>> async def main():
    >>>     async with trx:
    >>>         await trx.query("SELECT 1")
    >>>         await trx.commit()
    >>>
    >>>     # Output:
    >>>     # Transaction started
    >>>     # Query: SELECT 1 with args ()
    >>>     # Query executed with result <Result object at 0x7f5b0b5d6c10>
    >>>     # Transaction committed
    >>>     # Transaction ended
    >>>
    >>> asyncio.run(main())
    """

    def observe_query_before(self, conn: AioMySQLConnection, query: str, trx_context: Any, **kwargs: Any) -> Any:  # noqa: B027
        """
        Called before the query is sent to server for execution.
        :param conn: MySQL connection that this query will be sent to.
        :param query: Query that will be executed, including all arguments already escaped and formatted.
        :param trx_context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional arguments passed to the query execution method.
        :return: Any context object that will be passed to observe_query_after and observe_query_error methods.
        """

    def observe_query_after(  # noqa: B027
        self, conn: AioMySQLConnection, query: str, context: Any, trx_context: Any, **kwargs: Any
    ) -> None:
        """
        Called after the query has been executed successfully.
        :param conn: MySQL connection that this query was executed on.
        :param query: Query that has been executed.
        :param context: Context object returned by observe_query_before method.
        :param trx_context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional kwargs passed to the query execution method.
        """

    def observe_query_error(  # noqa: B027
        self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, trx_context: Any, **kwargs: Any
    ) -> None:
        """
        Called when the query execution has failed.
        :param conn: MySQL connection that this query was executed on.
        :param query: Query that has been executed.
        :param error: Exception that was raised during query execution.
        :param context: Context object returned by observe_query_before method.
        :param trx_context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional kwargs passed to the query execution method.
        """

    def observe_transaction_start(self, trx: Transaction, **kwargs: Any) -> Any:  # noqa: B027
        """
        Called when the transaction is started.
        :param trx: Transaction object that is being started.
        :param kwargs: Additional kwargs passed to the transaction start method.
        :return: Any context object that will be passed to observe_transaction_end, observe_transaction_commit and observe
          transaction_rollback methods.
        """

    def observe_transaction_end(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        """
        Called when the transaction is ended either by commit or rollback.
        :param trx: Transaction object that is being ended.
        :param context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional kwargs passed to the transaction end method.
        """

    def observe_transaction_commit(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        """
        Called when the transaction is committed.
        :param trx: Transaction object that is being committed.
        :param context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional kwargs passed to the transaction commit method.
        """

    def observe_transaction_rollback(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        """
        Called when the transaction is rolled back.
        :param trx: Transaction object that is being rolled back.
        :param context: Context object returned by observe_transaction_start method.
        :param kwargs: Additional kwargs passed to the transaction rollback method.
        """


class LoggingObserver(QueryObserver):
    """
    Observer that logs all queries to the logger.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger to log queries to.
        """
        self.logger = logger

    def observe_query_before(
        self, conn: AioMySQLConnection, query: str, *, logger: Optional[logging.Logger] = None, stacklevel: int = 0, **kwargs: Any
    ) -> float:
        """Log start of query execution."""
        (logger or self.logger).debug("Executing %s", query, stacklevel=_SL.get() + stacklevel)
        return time()

    def observe_query_after(
        self,
        conn: AioMySQLConnection,
        query: str,
        context: float,
        *,
        logger: Optional[logging.Logger] = None,
        stacklevel: int = 0,
        **kwargs: Any,
    ) -> None:
        """Log end of query execution."""
        (logger or self.logger).debug("Took %1.4fs", time() - context, stacklevel=_SL.get() + stacklevel)

    def observe_query_error(
        self,
        conn: AioMySQLConnection,
        query: str,
        error: BaseException,
        context: float,
        *,
        logger: Optional[logging.Logger] = None,
        stacklevel: int = 0,
        **kwargs: Any,
    ) -> None:
        """Log query error."""
        (logger or self.logger).debug("Took %1.4fs: %s", time() - context, str(error), stacklevel=_SL.get() + stacklevel)


class ObserverContext:
    """
    Context object for storing observer state.
    """

    ctx: ClassVar[ContextVar[Optional[dict[str, Any]]]] = ContextVar[Optional[dict[str, Any]]](
        "ObserverContext.ctx", default=None
    )

    def __init__(self, **kwargs: Any):
        self._kwargs = kwargs
        self.token: Optional[Token[dict[str, Any] | None]] = None

    def __enter__(self) -> Self:
        existing_kwargs = self.__class__.ctx.get()
        if not existing_kwargs:
            existing_kwargs = {}

        self.token = self.__class__.ctx.set({**existing_kwargs, **self._kwargs})
        return self

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        try:
            if self.token:
                self.__class__.ctx.reset(self.token)
        except ValueError as exc:
            if exc_val:
                raise BaseExceptionGroup("Observer failed.", [exc, exc_val])  # pylint: disable=raise-missing-from  # noqa: B904

            raise

    def kwargs(self, **kwargs: Any) -> dict[str, Any]:
        """Combine keyword arguments from the context and the method call."""
        existing_kwargs = self.__class__.ctx.get()
        if not existing_kwargs:
            existing_kwargs = {}

        return {**existing_kwargs, **kwargs}


class _QueryObserverContext(ObserverContext):
    """Context object for storing query observer state during query execution."""

    query_ctx: ClassVar[ContextVar[Optional["_QueryObserverContext"]]] = ContextVar[Optional["_QueryObserverContext"]](
        "_QueryObserverContext.ctx", default=None
    )

    def __init__(self, conn: AioMySQLConnection, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.conn = conn
        self.sql: Optional[str] = None
        self.query_token = self.__class__.query_ctx.set(self)
        self.observer_context: dict[int, Any] = {}

    def __exit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        if self.sql is not None:
            if exc_val is not None:
                self.conn.observe_query_error(self.sql, exc_val, self.observer_context, **self.kwargs())
            else:
                self.conn.observe_query_after(self.sql, self.observer_context, **self.kwargs())

        try:
            self.__class__.query_ctx.reset(self.query_token)
        except ValueError:
            pass

        super().__exit__(exc_type, exc_val, exc_tb)


class _TransactionQueryObserver(QueryObserver):
    """Helper class that is used by Transaction to observe queries and pass them to transaction observers."""

    def __init__(self, trx: Transaction) -> None:
        self.trx = trx

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:
        observer_context: dict[int, Any] = {}

        with _SL():
            for observer in self.trx.observers:
                observer_context[id(observer)] = observer.observe_query_before(
                    conn, query, self.trx.observer_context.get(id(observer)), **kwargs
                )

            return observer_context

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, **kwargs: Any) -> None:
        with _SL():
            for observer in self.trx.observers:
                observer.observe_query_after(
                    conn, query, context.get(id(observer)), self.trx.observer_context.get(id(observer)), **kwargs
                )

    def observe_query_error(
        self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, **kwargs: Any
    ) -> None:
        with _SL():
            for observer in self.trx.observers:
                observer.observe_query_error(
                    conn, query, error, context.get(id(observer)), self.trx.observer_context.get(id(observer)), **kwargs
                )
