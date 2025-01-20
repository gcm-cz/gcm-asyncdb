from __future__ import annotations

from enum import Enum
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, Self, Type, cast

from .._sl import _SL
from ..asyncdb import Transaction as AsyncTransactionBase
from ..exceptions import LogicError
from .cursor import Cursor
from .error import _query_error_factory
from .observer import ObserverContext, TransactionObserver, _TransactionQueryObserver
from .result import BoolResult, Result

if TYPE_CHECKING:
    from .connection import AioMySQLConnection


class TransactionIsolationLevel(str, Enum):
    """Transaction isolation level. See MySQL documentation for explanation."""

    REPEATABLE_READ = "REPEATABLE READ"
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    SERIALIZABLE = "SERIALIZABLE"


class Transaction(AsyncTransactionBase["AioMySQLConnection"]):  # pylint: disable=too-many-instance-attributes
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
        self.observer_context: dict[int, Any] = {}

        self.connection_observer = _TransactionQueryObserver(self)
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
        return cast(int, (await self.cursor).lastrowid)

    async def affected_rows(self) -> int:
        """Get number of affected rows by the last query."""
        await self._check_transaction_open("")
        return (await self.cursor).rowcount

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
    async def cursor(self) -> Cursor:
        """
        Get cursor for executing queries. Should not be needed, one should use query() method instead.
        """
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

            cursor = await self.cursor

            try:
                await cursor.execute(query, args)

            # pylint: disable=broad-exception-caught
            except Exception as exc:
                _query_error_factory(query, exc, self.connection)

            if cursor.description:
                self._cursor_clean = False
                self._last_result = Result(self, cursor, f"{query} with args {args}" if args else query)
                return self._last_result

            return BoolResult(self, True, f"{query} with args {args}" if args else query)

    @property
    def remote_app(self) -> str:
        """
        Remote app associated with the transaction to enhance error messages.
        """
        return self.connection.remote_app
