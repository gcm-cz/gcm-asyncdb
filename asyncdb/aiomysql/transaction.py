from __future__ import annotations

from collections.abc import Collection
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
        isolation_level: Optional[TransactionIsolationLevel] = None,
        observers: Optional[Collection[TransactionObserver]] = None,
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
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
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
    def isolation_level(self) -> Optional[TransactionIsolationLevel]:
        """
        Get current isolation level of this transaction.

        If property is None, default isolation level of the database is used. This level is not retrieved from the database.
        """
        return self._isolation_level

    async def _start_transaction(self) -> None:
        if self._transaction_initiated:
            raise LogicError("", "Transaction has already been started.", remote_app=self.remote_app)

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
        return cast(int, (await self._get_cursor()).lastrowid)

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
        return (await self._get_cursor()).rowcount

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
        with _SL(3):
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
        `Result` object instead (`Result.fetch_all_dict()`).

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
        `Result` object instead (`Result.fetch_all_dict()`).

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
