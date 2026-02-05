# pylint: disable=too-many-lines

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Collection, Iterable, Sequence
from functools import wraps
from types import TracebackType
from typing import (
    Any,
    ClassVar,
    Generic,
    Self,
    cast,
    overload,
)
from warnings import warn

from tenacity import RetryCallState, retry
from tenacity.retry import RetryBaseT, retry_any
from tenacity.stop import StopBaseT
from tenacity.wait import WaitBaseT

from asyncdb._sl import _SL
from asyncdb._sqlfactory_compat import ExecutableStatement
from asyncdb.asyncdb.bool_result import BoolResult
from asyncdb.asyncdb.observer import GenericTransactionObserver, _TransactionQueryObserver
from asyncdb.asyncdb.observers.logging_observer import TransactionLoggingObserver
from asyncdb.asyncdb.result import Result
from asyncdb.exceptions import EntityNotFound, ImplicitRollback, LogicError, SharedRollback, TooManyRowsError
from asyncdb.generics import OBJ, OBJ_OR_FACTORY, ConnectionTypeT, DictT, ListT, ResultRow, T
from asyncdb.types import SharedRollbackMode, _default, _DefaultPlaceholder


class Transaction(ABC, Generic[ConnectionTypeT]):
    # pylint: disable=too-many-instance-attributes
    """
    Abstract base class representing asynchronous transaction interface.

    Transaction is main access port to the database. By design, asyncdb enforces strong transaction usage for accessing the
    database. This is to ensure that all operations are performed in a transactional context, and to prevent common
    pitfalls like forgetting to commit the transaction.

    Transaction is a context manager, so it can be used in `async with` statement. This ensures that transaction is
    always properly committed or rollbacked, even if an exception occurs during the transaction. After the context
    is left, the transaction is rolled back automatically. If you want to commit it, you must do it manually.

    Example:

    >>> import asyncio
    >>> from asyncdb import Transaction
    >>>
    >>> async def main():
    >>>     async with Transaction(...) as trx:
    >>>         # Run query on transaction. Query returns `Result` instance, which can be used to access the result set.
    >>>         await trx.query("SELECT 1")
    >>>         # Note the missing commit here.
    >>>     # Warning: ImplicitRollback: Rolling back uncommited transaction.
    >>>
    >>>     async with Transaction(...) as trx:
    >>>         await trx.query("SELECT 1")
    >>>         await trx.commit()
    >>>     # No warning here, transaction is commited.
    >>>
    >>> asyncio.run(main())
    """

    __trx_id_counter__: ClassVar[int] = 0

    def __init__(
        self,
        connection: ConnectionTypeT,
        observers: Collection[GenericTransactionObserver[Any, Any, ConnectionTypeT, Transaction]] | None = None,
        *,
        retry_stop: StopBaseT | None = None,
        retry_wait: WaitBaseT | None = None,
        retry_retry: RetryBaseT | None = None,
        shared_rollback_mode: SharedRollbackMode = SharedRollbackMode.WARN,
    ) -> None:
        """
        :param connection: Connection this transaction should use.
        :param observers: Collection of observers to attach to the transaction.
        :param shared_rollback_mode: What to do when shared transaction issues rollback.
        """
        self._trx_id = Transaction.__trx_id_counter__ = Transaction.__trx_id_counter__ + 1

        self._connection = connection
        self.__shared_can_commit__: bool = True
        self.__shared_mode__: SharedRollbackMode = shared_rollback_mode

        self.observers: set[GenericTransactionObserver[Any, Any, Any, Any]] = set()
        """Observers attached to the transaction."""

        self.observer_context: dict[int, Any] = {}
        """Context for passing data between observers. Accessed from the observer implementation, do not modify directly."""

        self.connection_observer = _TransactionQueryObserver(self)
        """Observer bound to connection, to pass callbacks to transaction observers."""

        self.attach(TransactionLoggingObserver(logger=self.connection.logger))

        if observers:
            for observer in observers:
                self.attach(observer)

        self._retry_stop = retry_stop
        self._retry_wait = retry_wait
        self._retry_retry = retry_retry

    @property
    def trx_id(self) -> int:
        """
        Get unique transaction ID.
        """
        return self._trx_id

    def attach(self, observer: GenericTransactionObserver[Any, Any, ConnectionTypeT, Transaction]) -> None:
        """
        Attach observer to the transaction.

        Example:

        >>> import asyncio
        >>> from asyncdb.asyncdb import ConnectionProtocol, Transaction, TransactionObserver
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
        >>> connection: ConnectionProtocol = ...
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

    def detach(self, observer: GenericTransactionObserver[Any, Any, ConnectionTypeT, Transaction]) -> None:
        """
        Detach observer from the transaction.
        """
        self.observers.remove(observer)

    def __enter__(self) -> None:
        """
        Asynchronous transaction cannot be used in synchronous context.
        """
        raise RuntimeError("Cannot use async transaction in sync context. Use async with ...")

    async def __aenter__(self) -> Self:
        """
        Enter asynchronous transaction context.

        Example:

        >>> import asyncio
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.commit()
        >>>
        >>> asyncio.run(main())
        """
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> None:
        """
        If transaction is not explicitly committed or rollbacked, it is rollbacked as cleanup strategy.

        Example:

        >>> import asyncio
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         await trx.query("SELECT 1")
        >>>     # Warning: ImplicitRollback: Rolling back uncommited transaction.
        >>>
        >>> asyncio.run(main())
        """
        if not await self.is_committed():
            with _SL():
                warn("Rolling back uncommitted transaction.", ImplicitRollback, stacklevel=_SL.get() + 2)
                await self.rollback()

    @abstractmethod
    async def is_committed(self) -> bool:
        """
        Return `True` if transaction has been commited. False otherwise.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
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

        :return: True if transaction has been committed, False otherwise (return False since start until committed).
        """

    @abstractmethod
    async def last_insert_id(self) -> int:
        """
        Return last used AUTO_INCREMENT value.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
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

    @overload
    async def query(self, query: str, *args: Any, **kwargs: Any) -> Result: ...

    @overload
    async def query(self, query: ExecutableStatement) -> Result: ...

    @abstractmethod
    async def query(self, query: str | ExecutableStatement, *args: Any, **kwargs: Any) -> Result:
        """
        Execute single query inside the transaction scope, substituting all %s placeholders in the query string with
        provided arguments in the variadic arguments.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
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
        :param kwargs: Some drivers supports also specifying keyword arguments for the query, but most does not.
        :return: `Result` if query returns rows, or `BoolResult` if query succeeded but did not return any resultset.
        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error processing the query.
        """

    @abstractmethod
    async def affected_rows(self) -> int:
        """
        Return number of affected rows by most recent query.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
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

    async def commit(self) -> None:
        """
        Commit the transaction. After commit, no other operation can be performed on the transaction.

        Example:

        >>> import asyncio
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.commit()
        >>>
        >>>         await trx.query(...)   # raises LogicError: Query after commit
        >>>
        >>> asyncio.run(main())

        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error during commit.
        """
        if not self.__shared_can_commit__ and self.__shared_mode__ != SharedRollbackMode.IGNORE:
            with _SL():
                await self.rollback()

                match self.__shared_mode__:
                    case SharedRollbackMode.WARN:
                        warn(
                            "Shared transaction rollback propagated to main transaction.",
                            SharedRollback,
                            stacklevel=_SL.get() + 2,
                        )

                    case SharedRollbackMode.RAISE:
                        raise SharedRollback("Shared transaction rollback propagated to main transaction.")

            return

        with _SL():
            await self._actual_commit()

    @abstractmethod
    async def _actual_commit(self) -> None:
        """
        Actual commit implementation. This is called only if the transaction can be committed.
        """

    @abstractmethod
    async def rollback(self) -> None:
        """
        Rollback the transaction. After rollback, no other operation can be performed on the transaction.

        Example:

        >>> import asyncio
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         await trx.query("SELECT 1")
        >>>         await trx.rollback()
        >>>
        >>>         await trx.query(...)   # raises LogicError: Query after commit
        >>>
        >>> asyncio.run(main())

        :raises: `asyncdb.exceptions.QueryError` (or subclass of) if there was an error during rollback.
        """

    async def _get(
        self,
        result_fetcher: Callable[..., Callable[..., Awaitable[Any]]],
        query: str | ExecutableStatement,
        args: Iterable[Any] | None | type[T] | Callable[..., T],
        cls: type[T] | Callable[..., T],
        *cls_args: Any,
        **cls_kwargs: Any,
    ) -> T:
        if isinstance(query, ExecutableStatement) and args is not None:
            # When sqlfactory statement is used, the call looks like
            # _get(..., query=Statement, args=Factory, cls=first_arg, *cls_args, **cls_kwargs)

            if not isinstance(cls, _DefaultPlaceholder):
                # Shift cls to cls_args.
                cls_args = [cls, *cls_args]  # type: ignore
            else:
                # Extract the value from default placeholder, because it contains the type which should be instantiated.
                cls = cls.value  # type: ignore

            if not isinstance(args, _DefaultPlaceholder):
                cls = args  # type: ignore

            args = None
        else:
            if isinstance(cls, _DefaultPlaceholder):
                cls = cls.value  # type: ignore

            if isinstance(args, _DefaultPlaceholder):
                args = args.value  # type: ignore

        with _SL():
            if isinstance(query, ExecutableStatement):
                res = await self.query(query)
            else:
                res = await self.query(query, *(cast(Iterable[Any] | None, args) or []))

            if not isinstance(res, Result) or isinstance(res, BoolResult):
                raise LogicError(str(query), "Query did not return result.")

            row = await result_fetcher(res)(cls, *cls_args, **cls_kwargs)
            if row is None:
                raise EntityNotFound(str(query), "Requested entity was not found.")

            another = await result_fetcher(res)(cls, *cls_args, **cls_kwargs)
            if another is not None:
                raise TooManyRowsError(str(query), "Query returned more than one row.")

            return row

    @overload
    async def get_object(self, query: str, args: Iterable[Any] | None = None, /) -> ResultRow:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object("SELECT `id`, `name` FROM `products` WHERE `id` = %s", [1])
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
        :return: Object retrieved by the query.
        :raise: `asyncdb.exceptions.LogicError` when query returns more than one row.
        """

    @overload
    async def get_object(self, query: ExecutableStatement, /) -> ResultRow:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>> from sqlfactory import Select, Eq
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object(Select("id", "name", table="products", where=Eq("id", 1)))
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :return: Object retrieved by the query.
        :raise: `asyncdb.exceptions.LogicError` when query returns more than one row.
        """

    @overload
    async def get_object(
        self, query: str, args: Iterable[Any] | None = None, cls: type[OBJ] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> OBJ:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()` of specified type.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>> from dataclasses import dataclass
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object(
        >>>             "SELECT `id`, `name` FROM `products` WHERE `id` = %s",  # Query to be executed
        >>>             [1],            # Query arguments as collection
        >>>             Product,        # Type that will be fetched
        >>>             as_kwargs=True  # Pass columns from query as kwargs to Product constructor.
        >>>         )
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
        :param cls: Class representing the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """

    @overload
    async def get_object(self, query: ExecutableStatement, cls: type[OBJ] = ..., /, *cls_args: Any, **cls_kwargs: Any) -> OBJ:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()` of specified type.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>> from dataclasses import dataclass
        >>> from sqlfactory import Select, Eq
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object(
        >>>             Select("id", "name", table="products", where=Eq("id", 1)),  # Query to be executed
        >>>             Product,        # Type that will be fetched
        >>>             as_kwargs=True  # Pass columns from query as kwargs to Product constructor.
        >>>         )
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
        :param cls: Class representing the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """

    @overload
    async def get_object(
        self,
        query: str,
        args: Iterable[Any] | None = None,
        cls: Callable[..., OBJ] = ...,
        /,
        *cls_args: Any,
        as_kwargs: bool | None = None,
        **cls_kwargs: Any,
    ) -> OBJ:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()` of specified type.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>> from dataclasses import dataclass
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object(
        >>>             "SELECT `id`, `name` FROM `products` WHERE `id` = %s",  # Query to be executed
        >>>             [1],                # Query arguments as collection
        >>>             Product,            # Type that will be fetched
        >>>             as_kwargs=True,     # Pass columns from query as kwargs to Product constructor.
        >>>         )
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
        :param cls: Factory creating the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """

    @overload
    async def get_object(
        self,
        query: ExecutableStatement,
        cls: Callable[..., OBJ] = ...,
        /,
        *cls_args: Any,
        as_kwargs: bool | None = None,
        **cls_kwargs: Any,
    ) -> OBJ:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()` of specified type.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>> from dataclasses import dataclass
        >>> from sqlfactory import Select, Eq
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object(
        >>>             Select("id", "name", table="products", where=Eq("id", 1)),  # Query to be executed
        >>>             Product,            # Type that will be fetched
        >>>             as_kwargs=True,     # Pass columns from query as kwargs to Product constructor.
        >>>         )
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to be executed
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
        :param cls: Factory creating the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """

    async def get_object(
        self,
        query: str | ExecutableStatement,
        args: Iterable[Any] | None | OBJ_OR_FACTORY[OBJ] | type[ResultRow] = _default(None),  # noqa: B008
        cls: OBJ_OR_FACTORY[OBJ] | type[ResultRow] = _default(ResultRow),  # noqa: B008
        /,
        *cls_args: Any,
        as_kwargs: bool | None = None,
        **cls_kwargs: Any,
    ) -> OBJ | ResultRow:
        """
        Retrieve single object from the database. The query must return exactly one row, which will be
        converted to object using standard `Result.fetch_object()`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_object("SELECT `id`, `name` FROM `products` WHERE `id` = %s", [1])
        >>>         print(row)
        >>>         # ResultRow(id=1, name="Product 1")
        >>>
        >>> asyncio.run(main())

        :param query: Query to execute (or sqlfactory statement)
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call). Only
          if query is str. If query is sqlfactory statement, this argument is used as `cls`.
        :param cls: See `Result.fetch_object()` for details
        :param cls_args: See `Result.fetch_object()` for details
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param cls_kwargs: See `Result.fetch_object()` for details
        :return: Object of type specified by `cls` retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """
        with _SL():
            return await self._get(
                lambda res: res.fetch_object,
                query,
                args,
                cls,
                *cls_args,
                as_kwargs=as_kwargs,
                **cls_kwargs,
            )

    @overload
    async def get_dict(self, query: str, args: Iterable[Any] | None = None, /) -> dict[str, Any]: ...

    @overload
    async def get_dict(self, query: ExecutableStatement, /) -> dict[str, Any]: ...

    @overload
    async def get_dict(
        self, query: str, args: Iterable[Any] | None = None, cls: type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> DictT: ...

    @overload
    async def get_dict(
        self, query: ExecutableStatement, cls: type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> DictT: ...

    async def get_dict(
        self,
        query: str | ExecutableStatement,
        args: Iterable[Any] | None | type[DictT | dict[str, Any]] = _default(None),  # noqa: B008
        cls: type[DictT | dict[str, Any]] = _default(dict[str, Any]),  # noqa: B008
        /,
        *cls_args: Any,
        **cls_kwargs: Any,
    ) -> DictT | dict[str, Any]:
        """
        Retrieve single row from the database as dict. The query must return exactly one row, which will be
        converted to dict using standard `Result.fetch_dict()`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_dict("SELECT `id`, `name` FROM `products` WHERE `id` = %s", [1])
        >>>         print(row)
        >>>         # {"id": 1, "name": "Product 1"}
        >>>
        >>> asyncio.run(main())

        :param query: Query to execute (or sqlfactory statement)
        :param args: Arguments to the query. If query is sqlfactory statement, this argument is used as `cls`.
        :param cls: See Result.fetch_dict for details
        :param cls_args: See Result.fetch_dict for details
        :param cls_kwargs: See Result.fetch_dict for details
        :return: Dict retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """
        with _SL():
            return await self._get(lambda res: res.fetch_dict, query, args, cls, *cls_args, **cls_kwargs)

    @overload
    async def get_list(self, query: str, args: Iterable[Any] | None = None, /) -> list[Any]: ...

    @overload
    async def get_list(self, query: ExecutableStatement, /) -> list[Any]: ...

    @overload
    async def get_list(
        self, query: str, args: Iterable[Any] | None = None, cls: type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> ListT: ...

    @overload
    async def get_list(
        self, query: ExecutableStatement, cls: type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> ListT: ...

    async def get_list(
        self,
        query: str | ExecutableStatement,
        args: Iterable[Any] | None | type[ListT | list[Any]] = _default(None),  # noqa: B008
        cls: type[ListT | list[Any]] = _default(list),  # noqa: B008
        /,
        *cls_args: Any,
        **cls_kwargs: Any,
    ) -> ListT | list[Any]:
        """
        Retrieve single row from the database as list. The query must return exactly one row, which will be
        converted to list using standard `Result.fetch_list()`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         row = await trx.get_list(
        >>>             "SELECT MIN(`price`), MAX(`price`), AVG(`price`) FROM `products` WHERE `id` IN (%s, %s, %s)",
        >>>             [1, 2, 3]
        >>>         )
        >>>         print(row)
        >>>         # [100, 300, 187.5]
        >>>
        >>> asyncio.run(main())

        :param query: Query to execute (or sqlfactory statement)
        :param args: Arguments to the query. If query is sqlfactory statement, this argument is used as `cls`.
        :param cls: See Result.fetch_list for details
        :param cls_args: See Result.fetch_list for details
        :param cls_kwargs: See Result.fetch_list for details
        :return: List retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """
        with _SL():
            return await self._get(lambda res: res.fetch_list, query, args, cls, *cls_args, **cls_kwargs)

    @overload
    async def get_scalar(self, query: str, args: Iterable[Any] | None = None) -> Any: ...

    @overload
    async def get_scalar(self, query: ExecutableStatement) -> Any: ...

    async def get_scalar(self, query: str | ExecutableStatement, args: Iterable[Any] | None = None) -> Any:
        """
        Retrieve single value from the database. The query must return exactly one row and that
        row must contain exactly one column, which is then returned from this method.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction
        >>>
        >>> async def count_products(trx: Transaction, min_price: int) -> int:
        >>>     return await trx.get_scalar("SELECT COUNT(*) FROM `products` WHERE `price` >= %s", [min_price])
        >>>
        >>> async def main():
        >>>     async with Transaction(...) as trx:
        >>>         print(await count_products(trx, 100))
        >>>         # 5
        >>>
        >>> asyncio.run(main())

        :param query: Query to execute (or sqlfactory statement)
        :param args: Query arguments. Only if query is str, ignored for sqlfactory statement.
        :return: Value of first column of the first row of the result set.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row or a row contains more than single column.
        """
        with _SL():
            if isinstance(query, ExecutableStatement):
                row: Sequence[Any] = await self.get_list(query)
            else:
                row = await self.get_list(query, args)

            if len(row) != 1:
                raise TooManyRowsError(str(query), "Result must contain only one column to be fetched as scalar.")

            return row[0]

    @property
    def connection(self) -> ConnectionTypeT:
        """
        Return connection that this transaction is open on.
        :return: connection object
        """
        return self._connection

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Cleanup the connection associated with this transaction and prepare it for usage by next transaction.
        """

    @abstractmethod
    async def reset(self) -> None:
        """
        Reset the transaction object to be used again. Used to start new transaction over existing connection when retrying.
        """

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
                    if isinstance(observer, GenericTransactionObserver):
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
                retry=retry_any(self._retry_retry, retry_retry) if retry_retry else self._retry_retry,  # type: ignore[arg-type]
                wait=retry_wait or self._retry_wait,
                stop=retry_stop or self._retry_stop,
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
