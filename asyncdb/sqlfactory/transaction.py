from collections.abc import Iterable
from types import TracebackType
from typing import Any, Callable, Generic, Optional, Self, Type, cast, overload

from sqlfactory.execute import ExecutableStatement

from asyncdb import Result
from asyncdb import Transaction as TransactionBase
from asyncdb._sl import _SL
from asyncdb.generics import OBJ, OBJ_OR_FACTORY, ConnectionTypeT, DictT, ListT, ResultRow


class Transaction(Generic[ConnectionTypeT], TransactionBase[ConnectionTypeT]):
    """
    Transaction that allows to use sqlfactory's statements directly for executing queries.
    """

    def __init__(self, transaction: TransactionBase[ConnectionTypeT]) -> None:
        super().__init__(transaction.connection)
        self._transaction = transaction

    async def __aenter__(self) -> Self:
        await self._transaction.__aenter__()
        return self

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
    ) -> None:
        await self._transaction.__aexit__(exc_type, exc_val, exc_tb)

    async def last_insert_id(self) -> int:
        return await self._transaction.last_insert_id()

    @overload
    async def query(self, query: ExecutableStatement) -> Result: ...  # pylint: disable=arguments-differ

    @overload
    async def query(self, query: str, *args: Any, **kwargs: Any) -> Result: ...

    async def query(self, query: str | ExecutableStatement, *args: Any, **kwargs: Any) -> Result:
        if isinstance(query, ExecutableStatement):
            args = tuple(query.args)
            query = str(query)

        return await self._transaction.query(query, *args, **kwargs)

    async def affected_rows(self) -> int:
        return await self._transaction.affected_rows()

    async def commit(self) -> None:
        return await self._transaction.commit()

    async def rollback(self) -> None:
        return await self._transaction.rollback()

    @property
    def connection(self) -> ConnectionTypeT:
        return self._transaction.connection

    async def cleanup(self) -> None:
        return await self._transaction.cleanup()

    async def is_committed(self) -> bool:
        return await self._transaction.is_committed()

    @overload
    async def get_object(self, query: ExecutableStatement, /) -> ResultRow:  # pylint: disable=signature-differs
        """
        Execute sqlfactory's statement and return ResultRow.
        :param query: Query to be executed.
        :return: Object retrieved by the query.
        """

    @overload
    async def get_object(self, query: ExecutableStatement, cls: Type[OBJ] = ..., /, *cls_args: Any, **cls_kwargs: Any) -> OBJ:
        """
        Execute sqlfactory's statement and return object of type `cls`.
        :param query: Query to be executed.
        :param cls: Class representing the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        """

    @overload
    async def get_object(
        self,
        query: ExecutableStatement,
        cls: Callable[..., OBJ] = ...,
        /,
        *cls_args: Any,
        as_kwargs: bool = False,
        **cls_kwargs: Any,
    ) -> OBJ:
        """
        Execute sqlfactory's statement and return object created by `cls` factory..
        :param query: Query to be executed.
        :param cls: Factory creating the object that should be returned.
        :param cls_args: Optional arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param cls_kwargs: Optional keyword arguments to the `cls` constructor.
        :return: Object of type `cls` retrieved by the query.
        """

    @overload
    async def get_object(self, query: str, args: Optional[Iterable[Any]] = None, /) -> ResultRow:
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
    async def get_object(
        self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[OBJ] = ..., /, *cls_args: Any, **cls_kwargs: Any
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
    async def get_object(
        self,
        query: str,
        args: Optional[Iterable[Any]] = None,
        cls: Callable[..., OBJ] = ...,
        /,
        *cls_args: Any,
        as_kwargs: bool = False,
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

    async def get_object(
        self,
        query: str | ExecutableStatement,
        args: Optional[Iterable[Any]] | OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = None,
        cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow,
        /,
        *cls_args: Any,
        as_kwargs: bool = False,
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

        :param query: Query to execute
        :param args: Arguments to the query (as collection, not as variadic arguments - this differs from `query()` call).
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
        if isinstance(query, ExecutableStatement):
            # Shift second argument to correct position, as args are not present in sqlfactory overloads.
            cls = cast(OBJ_OR_FACTORY[OBJ] | Type[ResultRow], args)
            args = query.args
            query = str(query)

        with _SL():
            return await super().get_object(query, cast(Iterable[Any], args), cls, *cls_args, as_kwargs=as_kwargs, **cls_kwargs)

    @overload
    async def get_dict(self, query: ExecutableStatement, /) -> dict[str, Any]: ...  # pylint: disable=signature-differs

    @overload
    async def get_dict(
        self, query: ExecutableStatement, cls: Type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> DictT: ...

    @overload
    async def get_dict(self, query: str, args: Optional[Iterable[Any]] = None, /) -> dict[str, Any]: ...

    @overload
    async def get_dict(
        self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> DictT: ...

    async def get_dict(
        self,
        query: str | ExecutableStatement,
        args: Optional[Iterable[Any]] | Type[DictT | dict[str, Any]] = None,
        cls: Type[DictT | dict[str, Any]] = dict,
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

        :param query: Query to execute
        :param args: Arguments to the query
        :param cls: See Result.fetch_dict for details
        :param cls_args: See Result.fetch_dict for details
        :param cls_kwargs: See Result.fetch_dict for details
        :return: Dict retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """
        if isinstance(query, ExecutableStatement):
            # Shift second argument to correct position, as args are not present in sqlfactory overloads.
            cls = cast(Type[DictT | dict[str, Any]], args)
            args = query.args
            query = str(query)

        with _SL():
            return await super().get_dict(query, cast(Iterable[Any], args), cls, *cls_args, **cls_kwargs)

    @overload
    async def get_list(self, query: ExecutableStatement, /) -> list[Any]: ...  # pylint: disable=signature-differs

    @overload
    async def get_list(
        self, query: ExecutableStatement, cls: Type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> ListT: ...

    @overload
    async def get_list(self, query: str, args: Optional[Iterable[Any]] = None, /) -> list[Any]: ...

    @overload
    async def get_list(
        self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> ListT: ...

    async def get_list(
        self,
        query: str | ExecutableStatement,
        args: Optional[Iterable[Any] | Type[ListT | list[Any]]] = None,
        cls: Type[ListT | list[Any]] = list,
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

        :param query: Query to execute
        :param args: Arguments to the query
        :param cls: See Result.fetch_list for details
        :param cls_args: See Result.fetch_list for details
        :param cls_kwargs: See Result.fetch_list for details
        :return: List retrieved by the query.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row.
        """
        if isinstance(query, ExecutableStatement):
            # Shift second argument to correct position, as args are not present in sqlfactory overloads.
            cls = cast(Type[ListT | list[Any]], args)
            args = query.args
            query = str(query)

        with _SL():
            return await super().get_list(query, cast(Iterable[Any], args), cls, *cls_args, **cls_kwargs)

    @overload
    async def get_scalar(self, query: ExecutableStatement) -> Any: ...  # pylint: disable=arguments-differ

    @overload
    async def get_scalar(self, query: str, args: Optional[Iterable[Any]] = None) -> Any: ...

    async def get_scalar(self, query: str | ExecutableStatement, args: Optional[Iterable[Any]] = None) -> Any:
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

        :param query: Query to execute
        :param args: Query arguments.
        :return: Value of first column of the first row of the result set.
        :raises: `LogicError` if query did not return a result set (i.e. modifying query instead of SELECT).
        :raises: `EntityNotFound` if query returned a result set, but it was empty.
        :raises: `TooManyRowsError` if query returned more than single row or a row contains more than single column.
        """
        if isinstance(query, ExecutableStatement):
            args = query.args
            query = str(query)

        with _SL():
            return await super().get_scalar(query, args)
