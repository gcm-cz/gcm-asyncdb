from __future__ import annotations

import warnings
from collections.abc import Awaitable, Callable, Collection
from enum import Enum
from functools import wraps
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Self, overload
from warnings import warn

from aiomysql.connection import MySQLResult
from tenacity import RetryCallState, retry, retry_any
from tenacity.retry import RetryBaseT
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT
from typing_extensions import deprecated

from .._sl import _SL
from .._sqlfactory_compat import ExecutableStatement
from ..asyncdb import Transaction as AsyncTransactionBase
from ..exceptions import ImplicitRollback, LogicError
from ..generics import ArgsT
from .config import DEFAULT_RETRY, DEFAULT_RETRY_STOP, DEFAULT_RETRY_WAIT
from .error import _query_error_factory
from .observer import ObserverContext, TransactionObserver, _TransactionQueryObserver
from .observers.logging_observer import TransactionLoggingObserver
from .result import BoolResult, Result

if TYPE_CHECKING:
    from .connection import AioMySQLConnection  # pragma: no cover


class TransactionIsolationLevel(str, Enum):
    """Transaction isolation level. See MySQL documentation for explanation."""

    REPEATABLE_READ = "REPEATABLE READ"
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    SERIALIZABLE = "SERIALIZABLE"


class Transaction(AsyncTransactionBase["AioMySQLConnection"]):
    # pylint: disable=too-many-instance-attributes, too-many-public-methods
    """
    Single database transaction. This is main access port to the database. By design, asyncdb enforces strong transaction
    usage for accessing the database. This is to prevent common mistakes by forgetting to commit transaction, which can lead to
    inconsistencies when using single connection from multiple points.

    Transaction is created from connection and must be used as context manager. When you leave the context, the transaction is
    automatically rolled back. This is to prevent leaving transaction open by mistake. If you want to commit the transaction,
    you should do it manually within the context. After commit or rollback, the transaction is closed and cannot be used anymore.

    Example:

    >>> import asyncio
    >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
    >>>
    >>> db_connection: AioMySQLConnection = ...
    >>>
    >>> async def main():
    >>>     async with Transaction(db_connection) as trx:
    >>>         await trx.query("SELECT 1")
    >>>         # Note the missing commit here.
    >>>     # Warning: ImplicitRollback: Rolling back uncommited transaction.
    >>>
    >>>     async with Transaction(db_connection) as trx:
    >>>         await trx.query("SELECT 1")
    >>>         await trx.commit()
    >>>     # No warning here, transaction is commited.
    >>>
    >>> asyncio.run(main())
    """

    def __init__(
        self,
        connection: AioMySQLConnection,
        isolation_level: TransactionIsolationLevel | None = None,
        observers: Collection[TransactionObserver] | None = None,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
    ):
        """
        Create new transaction on top of existing AioMySQLConnection.

        This can be used as context manager to provide automatic rollback in case of error (recommended!). But the transaction
        will work even without context manager. In that case, your code must provide rollback functionality in case of error,
        otherwise the transaction will stay open.

        Example:

        >>> connection: AioMySQLConnection = ...
        >>>
        >>> # Transaction with default isolation level set by the database.
        >>> async with Transaction(connection) as trx:
        >>>     ...
        >>>
        >>> # Transaction with custom isolation level.
        >>> async with Transaction(connection, TransactionIsolationLevel.SERIALIZABLE) as trx:
        >>>     ...

        :param connection: Connection to use for the transaction. Remember, that only one transaction can be active on the
          connection at the time.
        :param isolation_level: Transaction isolation level to use. If None, default isolation level of the database is used.
        :param observers: Collection of observers to attach to the transaction.
        :param retry_stop: Optional tenacity stop condition for deadlock retries.
        :param retry_wait: Optional tenacity wait condition for deadlock retries.
        :param retry_retry: Optional tenacity retry condition for deadlock retries.
        """
        super().__init__(connection)

        self._isolation_level = isolation_level
        self._transaction_open = True
        self._transaction_initiated = False
        self._last_result: Result | None = None

        self._retry_stop = retry_stop
        self._retry_wait = retry_wait
        self._retry_retry = retry_retry

        self._cursor_clean = True

        self._lastrowid: int = 0  # Result of last insert ID.

        self.observers: set[TransactionObserver] = set()
        """Observers attached to the transaction."""

        self.observer_context: dict[int, Any] = {}
        """Context for passing data between observers. Accessed from the observer implementation, do not modify directly."""

        self.connection_observer = _TransactionQueryObserver(self)
        """Observer bound to connection, to pass callbacks to transaction observers."""

        self.attach(TransactionLoggingObserver(logger=self.connection.logger))

        if observers:
            for observer in observers:
                self.attach(observer)

    async def __aenter__(self) -> Self:
        """
        Start the transaction on context enter.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
        >>>
        >>> connection: AioMySQLConnection = ...
        >>>
        >>> async def main():
        >>>     async with Transaction(connection) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.commit()
        >>>
        >>> asyncio.run(main())
        """
        with _SL(2):
            await self._start_transaction()

        return await super().__aenter__()

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        Exit async context. Rollback the transaction if it is still open.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
        >>>
        >>> connection: AioMySQLConnection = ...
        >>>
        >>> async def main():
        >>>     async with Transaction(connection) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         # Note missing commit here.
        >>>     # Warning: ImplicitRollback: Rolling back uncommited transaction.
        >>>
        >>>     async with Transaction(connection) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.commit()
        >>>     # No warning issued, transaction is commited.
        >>>
        >>> asyncio.run(main())
        """
        with _SL():
            return await super().__aexit__(exc_type, exc_val, exc_tb)

    def attach(self, observer: TransactionObserver) -> None:
        """
        Attach observer to the transaction.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction, TransactionObserver
        >>>
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
        >>> async def main():
        >>>     trx = Transaction(connection)
        >>>     trx.attach(MyObserver())
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
        """
        self.observers.add(observer)

    def detach(self, observer: TransactionObserver) -> None:
        """
        Detach observer from the transaction.
        """
        self.observers.remove(observer)

    @property
    def isolation_level(self) -> TransactionIsolationLevel | None:
        """
        Get current isolation level of this transaction.

        If property is None, default isolation level of the database is used. This level is not retrieved from the database.
        """
        return self._isolation_level

    async def reset(self) -> None:
        """
        Reset the transaction object to be used again.
        """
        with _SL():
            await self.cleanup()

            if not await self.is_committed():
                warn("Rolling back uncommitted transaction.", ImplicitRollback, stacklevel=_SL.get() + 2)
                await self.rollback()

            self._transaction_initiated = False
            await self._start_transaction()

    async def _start_transaction(self) -> None:
        if self._transaction_initiated:
            raise LogicError("", "Transaction has already been started.", remote_app=self.remote_app)

        self.connection.attach(self.connection_observer)

        with _SL():
            with _SL():
                if not hasattr(self.connection, "connected_time"):
                    await self.connection._connect()  # pylint: disable=protected-access

                self._transaction_initiated = True

            if self._isolation_level is not None:
                await self._connection.query(f"SET TRANSACTION ISOLATION LEVEL {self._isolation_level.value}")

            with _SL(2):
                with ObserverContext() as ctx:
                    try:
                        await self._connection.begin()

                        for observer in self.observers:
                            self.observer_context[id(observer)] = observer.observe_transaction_start(self, **ctx.kwargs())

                    # pylint: disable=broad-exception-caught
                    except Exception as exc:
                        _query_error_factory("BEGIN", exc, self._connection)

        self._transaction_open = True

    async def is_committed(self) -> bool:
        """
        Check if the transaction is committed.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
        >>>
        >>> trx: Transaction = ...
        >>>
        >>> async def do_some_fetching(trx: Transaction): ...
        >>>
        >>> async def do_some_work() -> None:
        >>>     async with trx:
        >>>         await do_some_fetching(trx)
        >>>
        >>>         if not await trx.is_committed():
        >>>             await trx.commit()
        >>>
        >>> asyncio.run(do_some_work())
        """
        return not self._transaction_open

    async def last_insert_id(self) -> int:
        """
        Get last inserted ID for autoincrement column.

        Example:

        >>> import asyncio
        >>>
        >>> trx: Transaction = ...
        >>>
        >>> async def main():
        >>>     await trx.query("INSERT INTO `products` (`name`) VALUES ('Product 1')")
        >>>     print(await trx.last_insert_id())
        >>>     # 1
        >>>
        >>> asyncio.run(main())
        """
        await self._check_transaction_open("")
        return self._last_result._result.insert_id if self._last_result else 0  # pylint: disable=protected-access

    async def affected_rows(self) -> int:
        """
        Get number of affected rows by the last query.

        Example:

        >>> import asyncio
        >>>
        >>> trx: Transaction = ...
        >>>
        >>> async def main():
        >>>     await trx.query("DELETE FROM `products` WHERE `id` = %s", 123)
        >>>     print(await trx.affected_rows())
        >>>     # 1
        >>>
        >>> asyncio.run(main())
        """
        await self._check_transaction_open("")
        return self._connection.affected_rows()

    async def commit(self) -> None:
        """
        Commit transaction. After commit, no other operation can be performed on the transaction.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
        >>>
        >>> connection: AioMySQLConnection = ...
        >>>
        >>> async def main():
        >>>     async with Transaction(connection) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.commit()
        >>>
        >>>         await trx.query(...)  # raises LogicError: Query after commit.
        >>>
        >>> asyncio.run(main())
        """
        with _SL(2):
            with ObserverContext() as ctx:
                await self._check_transaction_open("COMMIT")
                await self.cleanup()

                try:
                    await self._connection.commit()

                    for observer in self.observers:
                        observer.observe_transaction_commit(self, self.observer_context.get(id(observer)), **ctx.kwargs())

                # pylint: disable=broad-exception-caught
                except Exception as exc:
                    _query_error_factory("COMMIT", exc, self.connection)

                finally:
                    self._transaction_open = False

                    self.connection.detach(self.connection_observer)

                    for observer in self.observers:
                        observer.observe_transaction_end(self, self.observer_context.get(id(observer)), **ctx.kwargs())

    async def rollback(self) -> None:
        """
        Rollback transaction. After rollback, no other transaction can be performed on the transaction.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import AioMySQLConnection, Transaction
        >>>
        >>> connection: AioMySQLConnection = ...
        >>>
        >>> async def main():
        >>>     async with Transaction(connection) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.rollback()
        >>>
        >>>         await trx.query(...)  # raises LogicError: Query after rollback.
        >>>
        >>> asyncio.run(main())
        """
        with _SL(2):
            with ObserverContext() as ctx:
                await self._check_transaction_open("ROLLBACK")
                await self.cleanup()

                try:
                    await self._connection.rollback()

                    for observer in self.observers:
                        observer.observe_transaction_rollback(self, self.observer_context.get(id(observer)), **ctx.kwargs())

                # pylint: disable=broad-exception-caught
                except Exception as exc:
                    _query_error_factory("ROLLBACK", exc, self.connection)

                finally:
                    self.connection.detach(self.connection_observer)

                    for observer in self.observers:
                        observer.observe_transaction_end(self, self.observer_context.get(id(observer)), **ctx.kwargs())

                    self._transaction_open = False

    async def _check_transaction_open(self, query: str | ExecutableStatement) -> None:
        """Raise an error when transaction has already been commited."""
        if not self._transaction_initiated:
            await self._start_transaction()

        if await self.is_committed():
            raise LogicError(str(query), "Cannot perform operation on committed transaction.", remote_app=self.remote_app)

    async def _clean_cursor(self, result: Result) -> None:
        if self._last_result != result:
            raise LogicError(
                "", "Commands out of sync: Trying to close different result than last executed.", remote_app=self.remote_app
            )

        self._last_result = None
        self._cursor_clean = True

    @property
    def connection(self) -> AioMySQLConnection:
        """Return connection associated with this transaction."""
        return self._connection

    async def cleanup(self) -> None:
        """
        Cleanup the transaction before the connection is returned to the pool. Called automatically from the connection pool
        logic, there should be no reason to call this manually.
        """
        if self._last_result:
            await self._last_result.close()

        if self._last_result:
            self.connection.logger.error("Result.close() did not reset _last_result in transaction.")

        if not self._cursor_clean:
            raise LogicError(
                "", "Commands out of sync: You must first process all rows from previous result.", remote_app=self.remote_app
            )

    def _escape_args(self, args: Any) -> Any:
        if isinstance(args, (tuple, list)):
            return tuple(self._connection.escape(arg) for arg in args)

        if isinstance(args, dict):
            return {key: self._connection.escape(val) for (key, val) in args.items()}

        # If it's not a dictionary let's try escaping it anyways.
        # Worst case it will throw a Value error
        return self._connection.escape(args)

    async def query(self, query: str | ExecutableStatement, *args: Any, **kwargs: Any) -> Result:
        """
        Execute single query inside the transaction scope, substituting all %s placeholders in the query string with
        provided arguments in the variadic arguments.

        Example:

        >>> import asyncio
        >>> from asyncdb.aiomysql import Transaction
        >>>
        >>> trx: Transaction = ...
        >>>
        >>> async def main():
        >>>     res = await trx.query("SELECT * FROM `products` WHERE `id` = %s OR `name` = %s", 123, "Product 1")
        >>>     async for row in res.fetch_all_dict():
        >>>         print(row)
        >>>     # Output:
        >>>     # {"id": 1, "name": "Product 1"}
        >>>     # {"id": 123, "name": "Product 123"}
        >>>
        >>> asyncio.run(main())

        :param query: Query to execute. Can contain %s for each arg from args.
        :param args: Arguments to the query
        :param kwargs: Not supported by underlying aiomysql, will raise an error if used.
        :return: `Result` instance if query returns any result set, or `BoolResult` if query succeeded but did not return any
            result set.
        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error processing the query.
        """
        await self._check_transaction_open(query)
        if not self._cursor_clean:
            raise LogicError(
                str(query),
                f"Commands out of sync: You must first process all rows from previous result {self._cursor_clean} {id(self)}.",
                remote_app=self.remote_app,
            )

        if kwargs:
            raise LogicError(str(query), "aiomysql does not support kwargs in query.", remote_app=self.remote_app)

        try:
            if isinstance(query, ExecutableStatement):
                query_pattern = str(query)
                query_args: Collection[Any] = query.args

            else:
                query_pattern = query
                query_args = args

            formatted_query = query_pattern % self._escape_args(query_args)

            with _SL(2):
                await self._connection.query(formatted_query, query_pattern=query_pattern, query_args=query_args)

        # pylint: disable=broad-exception-caught
        except Exception as exc:
            _query_error_factory(str(query), exc, self.connection)

        result: MySQLResult = self._connection._result  # pylint: disable=protected-access

        if result.warning_count > 0:
            ws = await self._connection.show_warnings()
            if ws:
                for warning in ws:
                    warnings.warn(str(warning[-1]), Warning, stacklevel=_SL.get() + 2)

        if result.description:
            self._cursor_clean = False
            self._last_result = Result(self, result, f"{query!s} with args {args}" if args else repr(query))

        else:
            self._last_result = BoolResult(self, True, result, f"{query!s} with args {args}" if args else repr(query))

        return self._last_result

    @property
    def remote_app(self) -> str:
        """
        Remote app associated with the transaction to enhance error messages.
        """
        return self.connection.remote_app

    ## DB-API 2.0 compatibility methods follows. They are deprecated and should not be used. ##

    @deprecated("Use Transaction.query() instead.")
    async def execute(self, query: str, args: tuple[Any, ...] | None = None) -> int | None:
        """
        Execute single query inside the transaction scope.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it.
        Use `Transaction.query()` instead, which returns result.

        :param query: Query to execute. Can contain %s for each arg from args.
        :param args: Arguments to the query
        :return: Number of affected rows.
        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error processing the query.
        """
        if args:
            await self.query(query, *args)
        else:
            await self.query(query)

        return await self.affected_rows()

    @deprecated("Use Transaction.query() instead.")
    async def executemany(self, query: str, args: list[tuple[Any, ...]]) -> int | None:
        """
        Execute query with multiple set of arguments.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it.
        Use `Transaction.query()` instead, which returns result.

        :param query: Query to execute. Can contain %s for each arg from args.
        :param args: List of tuples with arguments. Each item in the list is one set of arguments and will be executed as
          separate query.
        :return: Number of affected rows by all queries
        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error processing the query.
        """
        affected_rows = 0

        for arg in args:
            await self.query(query, *arg)
            affected_rows += await self.affected_rows()

        return affected_rows

    @deprecated("This is not needed in asyncdb, next set is advanced automatically.")
    async def nextset(self) -> None:
        """
        Advance to next result set. This is just for compatibility with DB-API 2.0, but it is not needed in asyncdb, as next set
        is advanced automatically.
        """

    @deprecated("Use Transaction.query() instead.")
    async def callproc(self, procname: str, args: ArgsT | None = None) -> ArgsT | None:
        """
        Call stored procedure with arguments.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it.
        Use `Transaction.query()` instead, which returns result.

        :param procname: Stored procedure to call.
        :param args: Tuple of arguments for the procedure.
        :return: Original arguments.
        """
        if args:
            await self.query(f"CALL {procname}({', '.join(['%s'] * len(args))})", args)
        else:
            await self.query(f"CALL {procname}")

        return args

    @deprecated("Use Result.fetch_dict() instead.")
    async def fetchone(self) -> dict[str, Any] | None:
        """
        Fetch one row from the last query result.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it. Use methods on the
        `Result` object instead (`Result.fetch_dict()`).

        :return: One row from the result as dictionary. None if there are no more rows.
        """
        if not self._last_result or isinstance(self._last_result, BoolResult):
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        return await self._last_result.fetch_dict()

    @deprecated("Use Result.fetch_all_dict() instead.")
    async def fetchmany(self, size: int | None = None) -> list[dict[str, Any]]:
        """
        Fetch multiple rows from the last query result.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it. Use methods on the
        `Result` object instead (`Result.fetch_all_dict()`).

        :param size: Number of rows to fetch. If None, fetch all rows.
        :return: List of rows from the result as list of dictionaries.
        """
        if not self._last_result or isinstance(self._last_result, BoolResult):
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        out: list[dict[str, Any]] = []

        if size is None:
            size = 1000

        while size > 0:
            row = await self._last_result.fetch_dict()
            if row is None:
                break

            out.append(row)
            size -= 1

        return out

    @deprecated("Use Result.fetch_all_dict() instead.")
    async def fetchall(self) -> list[dict[str, Any]]:
        """
        Fetch all rows from the last query result.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it. Use methods on the
        `Result` object instead (`Result.fetch_all_dict()`).

        :return: All rows from the result as list of dictionaries.
        """
        if not self._last_result or isinstance(self._last_result, BoolResult):
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        return [row async for row in self._last_result.fetch_all_dict()]

    @deprecated("Use Result.scroll() instead.")
    async def scroll(self, value: int, mode: Literal["relative", "absolute"] = "relative") -> None:
        """
        Scroll the position of the cursor in the result set.
        :param value: Amount of rows to advance if `mode = "absolute"` or offset from current position if `mode = "relative"`.
        :param mode: `absolute` or `relative` determining how the result set will be scrolled.
        """
        if not self._last_result or isinstance(self._last_result, BoolResult):
            raise LogicError("", "Last query did not produce result. Unable to scroll.", remote_app=self.remote_app)

        await self._last_result.scroll(value, mode)

    async def _observe_retry(self, retry_state: RetryCallState) -> None:
        """
        Observe the retry state and log it. This is called before each retry.

        :param retry_state: Retry state.
        """
        # Stacklevel 4 is for Tenacity's internals (3) + retry decorator (1).
        with _SL(4):
            if not await self.is_committed():
                warn("Rolling back uncommitted transaction.", ImplicitRollback, stacklevel=_SL.get())
                await self.rollback()

            with _SL(2):
                for observer in self.observers:
                    if isinstance(observer, TransactionObserver):
                        observer.observe_transaction_retry(self, self.observer_context.get(id(observer)), retry_state=retry_state)

    async def _retry_reset_transaction(self, retry_state: RetryCallState) -> None:
        """
        Resets the transaction before retrying. This is called before each retry.

        :param retry_state: Retry state.
        """
        # Don't reset anything on first attempt, the transaction shuld be clean.
        if retry_state.attempt_number == 1:
            return

        with _SL(5):
            await self.reset()

    @overload
    def retry(
        self,
        /,
        func: Callable[..., Awaitable[Any]] | None = None,
    ) -> Callable[..., Awaitable[Any]]: ...

    @overload
    def retry(
        self,
        /,
        *,
        retry_wait: WaitBaseT | None = None,
        retry_stop: StopBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]: ...

    def retry(
        self,
        /,
        func: Callable[..., Awaitable[Any]] | None = None,
        *,
        retry_wait: WaitBaseT | None = None,
        retry_stop: StopBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
    ) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]] | Callable[..., Awaitable[Any]]:
        """
        Constructs decorator to decorate methods that should be retried in case of deadlock. Note that you must retry the whole
        transaction, not just the query that caused the deadlock.

        Usage:

        >>> # Initialize transaction as usual
        >>> async with Transaction() as trx:
        >>>     @trx.retry  # @trx.deadlock_retry() works too.
        >>>     async def retry_block():
        >>>         # Transaction logic goes here.
        >>>         await trx.query("SELECT 1")
        >>>
        >>>     await retry_block()

        Optionally, you can pass retry strategies to customize the retry behavior:

        >>> from tenacity import wait_fixed, stop_after_attempt
        >>>
        >>> async with Transaction() as trx:
        >>>     @trx.retry(
        >>>         retry_wait=wait_fixed(2),  # Wait 2 seconds between retries
        >>>         retry_stop=stop_after_attempt(5)  # Stop after 5 attempts
        >>>     )
        >>>     async def retry_block():
        >>>         # Transaction logic goes here.
        >>>         await trx.query("SELECT 1")
        >>>
        >>>     await retry_block()

        :param retry_wait: Wait strategy for retries. If None, uses Transaction's default.
            See https://tenacity.readthedocs.io/en/latest/api.html#wait-functions for possible wait strategies.
        :param retry_stop: Stop strategy for retries. If None, uses Transaction's default.
            See https://tenacity.readthedocs.io/en/latest/api.html#stop-functions for possible stop strategies.
        :param retry_retry: Retry strategy for retries. If None, uses Transaction's default.
            Note that `Deadlock` is always retried. See https://tenacity.readthedocs.io/en/latest/api.html#retry-functions
            for possible retry strategies.
        :return: Function decorator to wrap the function to be retried, or decorated function if called without arguments.
        """

        if not retry_retry:
            retry_retry = self._retry_retry

        def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
            @retry(
                retry=retry_any(DEFAULT_RETRY, retry_retry) if retry_retry else DEFAULT_RETRY,  # type: ignore[arg-type]
                wait=retry_wait or self._retry_wait or DEFAULT_RETRY_WAIT,
                stop=retry_stop or self._retry_stop or DEFAULT_RETRY_STOP,
                reraise=True,
                before=self._retry_reset_transaction,
                before_sleep=self._observe_retry,
            )
            @wraps(func)
            async def decorated(*args: Any, **kwargs: Any) -> Any:
                return await func(*args, **kwargs)

            return decorated

        if func:
            return decorator(func)

        return decorator
