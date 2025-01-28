from __future__ import annotations

from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Optional, Self, Type, cast

from typing_extensions import deprecated

from .._sl import _SL
from ..asyncdb import Transaction as AsyncTransactionBase
from ..exceptions import LogicError
from ..generics import ArgsT
from .cursor import Cursor
from .error import _query_error_factory
from .observer import ObserverContext, TransactionObserver, _TransactionQueryObserver
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
    Single database transaction.
    """

    def __init__(self, connection: AioMySQLConnection, isolation_level: Optional[TransactionIsolationLevel] = None):
        """
        Create new transaction on top of existing AioMySQLConnection.

        This can be used as context manager to provide automatic rollback in case of error (recommended!). But the transaction
        will work even without context manager. In that case, your code must provide rollback functionality in case of error,
        otherwise the transaction will stay open.

        :param connection: Connection to use for the transaction. Remember, that only one transaction can be active on the
          connection at the time.
        :param isolation_level: Transaction isolation level to use. If None, default isolation level of the database is used.
        """
        super().__init__(connection)

        self._cursor: Optional[Cursor] = None

        self._isolation_level = isolation_level
        self._transaction_open = True
        self._transaction_initiated = False
        self._last_result: Optional[Result] = None

        self._cursor_clean = True

        self.observers: set[TransactionObserver] = set()
        """Observers attached to the transaction."""

        self.observer_context: dict[int, Any] = {}
        """Context for passing data between observers. Accessed from the observer implementation, do not modify directly."""

        self.connection_observer = _TransactionQueryObserver(self)
        """Observer bound to connection, to pass callbacks to transaction observers."""

        self.connection.attach(self.connection_observer)

    async def __aenter__(self) -> Self:
        """Start the transaction."""
        with _SL(3):
            await self._start_transaction()

        return await super().__aenter__()

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        """Rollback the transaction if it is still open."""
        with _SL():
            return await super().__aexit__(exc_type, exc_val, exc_tb)

    def attach(self, observer: TransactionObserver) -> None:
        """Attach observer to the transaction."""
        self.observers.add(observer)

    def detach(self, observer: TransactionObserver) -> None:
        """Detach observer from the transaction."""
        self.observers.remove(observer)

    @property
    def isolation_level(self) -> Optional[TransactionIsolationLevel]:
        """Get current isolation level of this transaction. If None, default isolation level of the database is used."""
        return self._isolation_level

    async def _start_transaction(self) -> None:
        if self._transaction_initiated:
            raise LogicError("", "Transaction has already been started.", remote_app=self.remote_app)

        if not hasattr(self.connection, "connected_time"):
            await self.connection._connect()  # pylint: disable=protected-access

        self._transaction_initiated = True

        if self._isolation_level is not None:
            await self._connection.query(f"SET TRANSACTION ISOLATION LEVEL {self._isolation_level.value}")

        with _SL():
            with ObserverContext() as ctx:
                try:
                    await self._connection.begin()

                    for observer in self.observers:
                        self.observer_context[id(observer)] = observer.observe_transaction_start(self, **ctx.kwargs())

                # pylint: disable=broad-exception-caught
                except Exception as exc:
                    _query_error_factory("BEGIN", exc, self._connection)

    async def is_committed(self) -> bool:
        """Check if the transaction is committed."""
        return not self._transaction_open

    async def last_insert_id(self) -> int:
        """Get last inserted ID for autoincrement column."""
        await self._check_transaction_open("")
        return cast(int, (await self._get_cursor()).lastrowid)

    async def affected_rows(self) -> int:
        """Get number of affected rows by the last query."""
        await self._check_transaction_open("")
        return (await self._get_cursor()).rowcount

    async def commit(self) -> None:
        """Commit transaction"""
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
        """Rollback transaction"""
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

    async def _check_transaction_open(self, query: str) -> None:
        """Raise an error when transaction has already been commited."""
        if not self._transaction_initiated:
            await self._start_transaction()

        if await self.is_committed():
            raise LogicError(query, "Cannot perform operation on committed transaction.", remote_app=self.remote_app)

    async def _clean_cursor(self, result: Result) -> None:
        if self._last_result != result:
            raise LogicError(
                "", "Commands out of sync: Trying to close different result than last executed.", remote_app=self.remote_app
            )

        self._last_result = None

        if self._cursor and not self._cursor.closed:
            await self._cursor.close()

        self._cursor = None
        self._cursor_clean = True

    @property
    def connection(self) -> AioMySQLConnection:
        """Return connection associated with this transaction."""
        return self._connection

    @property
    @deprecated("Do not access cursor directly, use Transaction.query() instead.")
    async def cursor(self) -> Cursor:
        """
        Get cursor for executing queries. Should not be needed, one should use query() method instead.
        """
        return await self._get_cursor()

    async def _get_cursor(self) -> Cursor:
        if not self._cursor:
            self._cursor = cast(Cursor, await self._connection.cursor())

        return self._cursor

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

        if self._cursor:
            await self._cursor.close()
            self._cursor = None

    async def query(self, query: str, *args: Any, **kwargs: Any) -> Result:
        """
        Execute single query inside the transaction scope.
        :param query: Query to execute. Can contain %s for each arg from args.
        :param args: Arguments to the query
        :param kwargs: Some drivers supports also specifying keyword arguments for the query, but most does not.
        :return: Result if query returns rows, or True if query succeeded.
        :raises: QueryError (or subclass of) if there was an error processing the query.
        """
        with _SL(2):
            await self._check_transaction_open(query)

            if not self._cursor_clean:
                raise LogicError(
                    query,
                    f"Commands out of sync: You must first process all rows from previous "
                    f"result {self._cursor_clean} {id(self)}.",
                    remote_app=self.remote_app,
                )

            if kwargs:
                raise LogicError(query, "aiomysql does not support kwargs in query.", remote_app=self.remote_app)

            cursor = await self._get_cursor()

            try:
                await cursor.execute(query, args)

            # pylint: disable=broad-exception-caught
            except Exception as exc:
                _query_error_factory(query, exc, self.connection)

            if cursor.description:
                self._cursor_clean = False
                self._last_result = Result(self, cursor, f"{query} with args {args}" if args else query)
                return self._last_result

            self._last_result = None
            return BoolResult(self, True, f"{query} with args {args}" if args else query)

    @property
    def remote_app(self) -> str:
        """
        Remote app associated with the transaction to enhance error messages.
        """
        return self.connection.remote_app

    ## DB-API 2.0 compatibility methods follows. They are deprecated and should not be used. ##

    @deprecated("Use Transaction.query() instead.")
    async def execute(self, query: str, args: Optional[tuple[Any, ...]] = None) -> int | None:
        """
        Execute single query inside the transaction scope.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it.
        Use `Transaction.query()` instead, which returns result.

        :param query: Query to execute. Can contain %s for each arg from args.
        :param args: Arguments to the query
        :return: Number of affected rows.
        :raises: QueryError (or subclass of) if there was an error processing the query.
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
    async def callproc(self, procname: str, args: Optional[ArgsT] = None) -> Optional[ArgsT]:
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
    async def fetchone(self) -> Optional[dict[str, Any]]:
        """
        Fetch one row from the last query result.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it. Use methods on the
        `Result` object instead (`Result.fetch_dict()`).

        :return: One row from the result as dictionary. None if there are no more rows.
        """
        if not self._last_result:
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        return await self._last_result.fetch_dict()

    @deprecated("Use Result.fetch_all_dict() instead.")
    async def fetchmany(self, size: Optional[int] = None) -> list[dict[str, Any]]:
        """
        Fetch multiple rows from the last query result.

        Deprecated. It is there just for compatibility with DB-API 2.0, but it is not recommended to use it. Use methods on the
        `Result` object instead (`Result.fetch_all_dict()`.

        :param size: Number of rows to fetch. If None, fetch all rows.
        :return: List of rows from the result as list of dictionaries.
        """
        if not self._last_result:
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        out: list[dict[str, Any]] = []

        if size is None:
            if self._last_result.cursor:
                size = self._last_result.cursor.arraysize
            else:
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
        `Result` object instead (`Result.fetch_all_dict()`.

        :return: All rows from the result as list of dictionaries.
        """
        if not self._last_result:
            raise LogicError("", "Last query did not produce result. Unable to fetch rows.", remote_app=self.remote_app)

        return [row async for row in self._last_result.fetch_all_dict()]

    @deprecated("Use Result.scroll() instead.")
    async def scroll(self, value: int, mode: Literal["relative", "absolute"] = "relative") -> None:
        """
        Scroll the position of the cursor in the result set.
        :param value: Amount of rows to advance if `mode = "absolute"` or offset from current position if `mode = "relative"`.
        :param mode: `absolute` or `relative` determining how the result set will be scrolled.
        """
        if not self._last_result:
            raise LogicError("", "Last query did not produce result. Unable to scroll.", remote_app=self.remote_app)

        await self._last_result.scroll(value, mode)
