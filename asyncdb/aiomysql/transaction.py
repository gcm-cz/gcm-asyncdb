from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, Optional, Any, Union

import aiomysql

from .._sl import _SL
from .error import _query_error_factory
from .observer import _TransactionQueryObserver, TransactionObserver, ObserverContext
from .result import Result
from .. import LogicError
from ..asyncdb import Transaction as AsyncTransactionBase

if TYPE_CHECKING:
    from .connection import AioMySQLConnection


class TransactionIsolationLevel(str, Enum):
    REPEATABLE_READ = "REPEATABLE READ"
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    SERIALIZABLE = "SERIALIZABLE"


class Transaction(AsyncTransactionBase):
    """
    Single database transaction.
    """

    def __init__(self, connection: AioMySQLConnection, isolation_level: Optional[TransactionIsolationLevel] = None):
        self._connection: AioMySQLConnection = connection
        self._cursor: Optional[aiomysql.Cursor] = None

        self._isolation_level = isolation_level
        self._transaction_open = True
        self._transaction_initiated = False
        self._last_result: Optional[Result] = None

        self._cursor_clean = True

        self.observers: set[TransactionObserver] = set()
        self.observer_context: dict[id, Any] = {}

        self.connection_observer = _TransactionQueryObserver(self)
        self.connection.attach(self.connection_observer)

    async def __aenter__(self) -> Transaction:
        with _SL(3):
            await self._start_transaction()

        return await super().__aenter__()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        with _SL():
            return await super().__aexit__(exc_type, exc_val, exc_tb)

    def attach(self, observer: TransactionObserver):
        self.observers.add(observer)

    def detach(self, observer: TransactionObserver):
        self.observers.remove(observer)

    @property
    def isolation_level(self) -> TransactionIsolationLevel:
        """
        Get isolation level of this transaction.
        """
        return self._isolation_level

    async def _start_transaction(self):
        if self._transaction_initiated:
            raise LogicError("", "Transaction has already been started.")

        self._transaction_initiated = True

        if self._isolation_level is not None:
            await self._connection.query("SET TRANSACTION ISOLATION LEVEL {}".format(self._isolation_level.value))

        with _SL():
            with ObserverContext() as ctx:
                try:
                    await self._connection.begin()

                    for observer in self.observers:
                        self.observer_context[id(observer)] = observer.observe_transaction_start(
                            self, **ctx.kwargs()
                        )
                except Exception as exc:
                    _query_error_factory("BEGIN", exc, self._connection)

    async def is_committed(self) -> bool:  # type: ignore[override]
        return not self._transaction_open

    async def last_insert_id(self) -> int:  # type: ignore[override]
        await self._check_transaction_open("")
        return (await self.cursor).lastrowid

    async def affected_rows(self) -> int:  # type: ignore[override]
        await self._check_transaction_open("")
        return (await self.cursor).rowcount

    async def commit(self) -> None:  # type: ignore[override]
        with _SL(2):
            with ObserverContext() as ctx:
                await self._check_transaction_open("COMMIT")
                await self.cleanup()

                try:
                    await self._connection.commit()

                    for observer in self.observers:
                        observer.observe_transaction_commit(
                            self,
                            self.observer_context.get(id(observer)),
                            **ctx.kwargs()
                        )
                except Exception as exc:
                    _query_error_factory("COMMIT", exc, self.connection)
                finally:
                    self._transaction_open = False

                    self.connection.detach(self.connection_observer)

                    for observer in self.observers:
                        observer.observe_transaction_end(
                            self,
                            self.observer_context.get(id(observer)),
                            **ctx.kwargs()
                        )

    async def rollback(self) -> None:  # type: ignore[override]
        with _SL(2):
            with ObserverContext() as ctx:
                await self._check_transaction_open("ROLLBACK")
                await self.cleanup()

                try:
                    await self._connection.rollback()

                    for observer in self.observers:
                        observer.observe_transaction_rollback(
                            self,
                            self.observer_context.get(id(observer)),
                            **ctx.kwargs()
                        )
                except Exception as exc:
                    _query_error_factory("ROLLBACK", exc, self.connection)

                finally:
                    self.connection.detach(self.connection_observer)

                    for observer in self.observers:
                        observer.observe_transaction_end(
                            self,
                            self.observer_context.get(id(observer)),
                            **ctx.kwargs()
                        )

                    self._transaction_open = False

    async def _check_transaction_open(self, query: str):
        if not self._transaction_initiated:
            await self._start_transaction()

        if await self.is_committed():
            raise LogicError(query, "Cannot perform operation on committed transaction.")

    async def _clean_cursor(self, result: Result):
        if self._last_result != result:
            raise LogicError("", f"Commands out of sync: Trying to close different result than last executed.")

        self._last_result = None

        if self._cursor and not self._cursor.closed:
            await self._cursor.close()

        self._cursor = None
        self._cursor_clean = True

    @property
    def connection(self) -> AioMySQLConnection:
        """
        Return connection associated with this transaction.
        """
        return self._connection

    @property
    async def cursor(self) -> aiomysql.Cursor:
        """
        Get cursor for executing queries. Should not be needed, one should use query() method instead.
        """
        if not self._cursor:
            self._cursor = await self._connection.cursor()

        return self._cursor

    async def cleanup(self):
        """
        Cleanup the transaction before the connection is returned to the pool
        """
        if self._last_result:
            await self._last_result.close()

        if self._last_result:
            self.connection.logger.error("Result.close() did not reset _last_result in transaction.")

        if not self._cursor_clean:
            raise LogicError("", "Commands out of sync: You must first process all rows from previous result.")

        if self._cursor:
            await self._cursor.close()
            self._cursor = None

        await super().cleanup()

    async def query(self, query: str, *args, **kwargs) -> Union[Result, bool]:
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
                raise LogicError(query, f"Commands out of sync: You must first process all rows from previous result {self._cursor_clean} {id(self)}.")

            if kwargs:
                raise LogicError(query, "aiomysql does not support kwargs in query.")

            cursor = await self.cursor

            try:
                await cursor.execute(query, args)
            except Exception as exc:
                _query_error_factory(query, exc, self.connection)

            if cursor.description:
                self._cursor_clean = False
                self._last_result = Result(self, cursor, f"{query} with args {args}" if args else query)
                return self._last_result

            return True
