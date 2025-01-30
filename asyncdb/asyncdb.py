"""
Base classes for working with database transactions and results. Each individual driver inherits from these classes.
"""

# pylint: disable=too-many-lines

import inspect
from abc import ABC, abstractmethod
from types import TracebackType
from typing import (
    Any,
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    Iterable,
    MutableSequence,
    MutableSet,
    Optional,
    Self,
    Sequence,
    Type,
    cast,
    overload,
)
from warnings import warn

from typing_extensions import deprecated

from ._sl import _SL
from .exceptions import EntityNotFound, Error, ImplicitRollback, LogicError, TooManyRowsError
from .generics import OBJ, OBJ_OR_FACTORY, ConnectionTypeT, DictT, ListT, ResObjT, ResultRow, SetT, T


class Result(ABC):
    """
    Abstract base class representing result of a query. The `Result` is returned after performing a query on the transaction
    using the `Transaction.query()` method. If the query returns result set, the `Result` instance is returned, otherwise
    `BoolResult` is returned.

    You should not need to instantiate this class manually, it is returned by the transaction's `Transaction.query()` method.

    Example:

    >>> import asyncio
    >>> from asyncdb import Transaction, Result
    >>>
    >>> async def main():
    >>>     trx: Transaction = ...
    >>>     q: Result = await trx.query("SELECT * FROM `products`")
    >>>
    >>>     async for row in q.fetch_all_dict():
    >>>         print(row)
    >>>     # Output:
    >>>     # {"id": 1, "name": "Product 1"}
    >>>
    >>> asyncio.run(main())
    """

    @overload
    async def fetch_list(self, /) -> Optional[list[Any]]:
        """
        Fetch one row from result as `list` (indexed column access).

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_list():
        >>>     print(row)
        >>>     # [1, "Product 1"]
        >>>
        >>>     row = await q.fetch_list()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :return: `list` with row data or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_list(self, cls: Type[ListT], /, *args: Any, **kwargs: Any) -> Optional[ListT]:
        """
        Fetch one row from result as list (indexed column access). The list type can be specified as the cls argument.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     class customlist(list):
        >>>         pass
        >>>
        >>>     row = await q.fetch_list(customlist):
        >>>     print(row)
        >>>     # [1, "Product 1"]
        >>>     print(type(row))
        >>>     # <class 'customlist'>
        >>>
        >>>     row = await q.fetch_list()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: List type that should be returned. Defaults to plain list.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs to the cls constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @abstractmethod
    async def fetch_list(self, cls: Type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any) -> Optional[ListT | list[Any]]:
        """
        Fetch one row from result as list (indexed column access). The list type can be specified as the cls argument,
        defaults to `list`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_list():
        >>>     print(row)
        >>>     # [1, "Product 1"]
        >>>
        >>>     row = await q.fetch_list()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: List type that should be returned. Defaults to plain list.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs to the cls constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_dict(self, /) -> Optional[dict[str, Any]]:
        """
        Fetch one row from result as dictionary. The dictionary type can be specified as the cls argument.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_dict():
        >>>     print(row)
        >>>     # {"id": 1, "name": "Product 1"}
        >>>
        >>>     row = await q.fetch_dict()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :return: dict containing mapping from column names to values or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_dict(self, cls: Type[DictT], /, *args: Any, **kwargs: Any) -> Optional[DictT]:
        """
        Fetch one row from result as dictionary. The dictionary type can be specified as the cls argument.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     class customdict(dict):
        >>>         pass
        >>>
        >>>     row = await q.fetch_dict(customdict):
        >>>     print(row)
        >>>     # {"id": 1, "name": "Product 1"}
        >>>     print(type(row))
        >>>     # <class 'customdict'>
        >>>
        >>>     row = await q.fetch_dict()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: Dictionary type that should be returned.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @abstractmethod
    async def fetch_dict(
        self, cls: Type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, **kwargs: Any
    ) -> Optional[DictT | dict[str, Any]]:
        """
        Fetch one row from result as dictionary (mapping). The dictionary type can be specified as the cls argument. Defaults
        to `dict`.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_dict():
        >>>     print(row)
        >>>     # {"id": 1, "name": "Product 1"}
        >>>
        >>>     row = await q.fetch_dict()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: Dictionary type that should be returned. Defaults to plain dict.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_object(self, /) -> Optional[ResultRow]:
        """
        Fetch one row from result as `ResultRow` objects.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_object():
        >>>     print(f"ID: {row.id}, Name: {row.name}")
        >>>     # ID: 1, Name: Product 1
        >>>     print(type(row))
        >>>     # <class 'ResultRow'>
        >>>
        >>>     row = await q.fetch_object()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :return: Instance of cls created from result row or `None` if no rows are remaining in the result set.
        """

    @overload
    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> Optional[OBJ]:
        """
        Fetch one row from result as object of type specified by the cls argument.

        Example:

        >>> import asyncio
        >>> from dataclasses import dataclass
        >>> from asyncdb import Transaction, Result
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_object(as_kwargs=True):
        >>>     print(f"ID: {row.id}, Name: {row.name}")
        >>>     # ID: 1, Name: Product 1
        >>>     print(type(row))
        >>>     # <class 'Product'>
        >>>
        >>>     row = await q.fetch_object(as_kwargs=True)
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: Class (or factory) representing the object that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @abstractmethod
    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow, /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> Optional[OBJ | ResultRow]:
        """
        Fetch one row from result as object of type specified by the cls argument. Defaults to generic `ResultRow` object.

        Example:

        >>> import asyncio
        >>> from dataclasses import dataclass
        >>> from asyncdb import Transaction, Result
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products` LIMIT 1")
        >>>
        >>>     row = await q.fetch_object(Product, as_kwargs=True):
        >>>     print(f"ID: {row.id}, Name: {row.name}")
        >>>     # ID: 1, Name: Product 1
        >>>     print(type(row))
        >>>     # <class 'Product'>
        >>>
        >>>     row = await q.fetch_object()
        >>>     print(row)
        >>>     # None  - no more rows.
        >>>
        >>> asyncio.run(main())

        :param cls: Class (or factory) representing the object that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @staticmethod
    async def _fetch_all(
        method: Callable[[Any, Any, Any], Awaitable[Any]], cls: Type[OBJ] | Callable[..., OBJ], *args: Any, **kwargs: Any
    ) -> AsyncGenerator[OBJ, None]:
        while True:
            row = await method(cls, *args, **kwargs)

            if row is None:
                break

            yield row

    @overload
    def fetch_all_object(self, /, *, as_kwargs: bool = False) -> AsyncGenerator[ResultRow, None]:
        """
        Generate objects of type `ResultRow` for each row of the result set.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_object()
        >>>         print(f"ID: {row.id}, Name: {row.name}")
        >>>     # Output:
        >>>     # ID: 1, Name: Product 1
        >>>     # ID: 2, Name: Product 2
        >>>
        >>> asyncio.run(main())

        :return: Generator generating result rows as ResultRow objects.
        """

    @overload
    def fetch_all_object(
        self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> AsyncGenerator[OBJ, None]:
        """
        Generate objects of type specified by the `cls` for each row of the result set, with optionally passing args and kwargs
        to the `cls` constructor.

        Example:

        >>> import asyncio
        >>> from dataclasses import dataclass
        >>> from asyncdb import Transaction, Result
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_object(Product, as_kwargs=True)
        >>>         print(f"ID: {row.id}, Name: {row.name}")
        >>>     # Output:
        >>>     # ID: 1, Name: Product 1
        >>>     # ID: 2, Name: Product 2
        >>>
        >>> asyncio.run(main())

        :param cls: Class (or factory) representing the object that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as objects of specified type.
        """

    async def fetch_all_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow, /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> AsyncGenerator[ResultRow | OBJ, None]:
        """
        Generate objects of type specified by the `cls` for each row of the result set, with optionally passing args and kwargs
        to the `cls` constructor. Defaults to `ResultRow` generic object.

        Example:

        >>> import asyncio
        >>> from dataclasses import dataclass
        >>> from asyncdb import Transaction, Result
        >>>
        >>> @dataclass
        >>> class Product:
        >>>     id: int
        >>>     name: str
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_object(Product, as_kwargs=True)
        >>>         print(f"ID: {row.id}, Name: {row.name}")
        >>>     # Output:
        >>>     # ID: 1, Name: Product 1
        >>>     # ID: 2, Name: Product 2
        >>>
        >>> asyncio.run(main())

        :param cls: Class (or factory) representing the object that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as objects of specified type.
        """
        async for row in self._fetch_all(self.fetch_object, cls, *args, as_kwargs=as_kwargs, **kwargs):
            yield cast(OBJ, row)

    @overload
    def fetch_all_dict(self, /) -> AsyncGenerator[dict[str, Any], None]:
        """
        Generate dicts for each row of the result set.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_dict()
        >>>         print(row)
        >>>     # Output:
        >>>     # {"id": 1, "name": "Product 1"}
        >>>     # {"id": 2, "name": "Product 2"}
        >>>
        >>> asyncio.run(main())

        :return: Generator generating result rows as dicts.
        """

    @overload
    def fetch_all_dict(self, cls: Type[DictT], /, *args: Any, **kwargs: Any) -> AsyncGenerator[DictT, None]:
        """
        Generate dicts of type `cls` for each row of the result set, with optionally passing args and kwargs to the
        cls constructor.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     class customdict(dict):
        >>>         pass
        >>>
        >>>     async for row in q.fetch_all_dict(customdict)
        >>>         print(row)
        >>>     # Output:
        >>>     # {"id": 1, "name": "Product 1"}
        >>>     # {"id": 2, "name": "Product 2"}
        >>>
        >>> asyncio.run(main())

        :param cls: Dictionary type that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as dicts of specified type.
        """

    async def fetch_all_dict(
        self, cls: Type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, **kwargs: Any
    ) -> AsyncGenerator[DictT | dict[str, Any], None]:
        """
        Generate dicts of type `cls` for each row of the result set, with optionally passing args and kwargs to the
        cls constructor. Defaults to plain `dict`.

        Example:

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_dict()
        >>>         print(row)
        >>>     # Output:
        >>>     # {"id": 1, "name": "Product 1"}
        >>>     # {"id": 2, "name": "Product 2"}
        >>>
        >>> asyncio.run(main())

        :param cls: Dictionary type that should be returned.
        :param args: Arguments to the `cls` constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as dicts of specified type.
        """
        async for row in self._fetch_all(self.fetch_dict, cls, *args, **kwargs):
            yield cast(DictT, row)

    @overload
    def fetch_all_list(self, /) -> AsyncGenerator[list[Any], None]:
        """
        Generate `list` with row data for each row of the result set.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_list()
        >>>         print(row)
        >>>     # Output:
        >>>     # [1, "Product 1"]
        >>>     # [2, "Product 2"]
        >>>
        >>> asyncio.run(main())

        :return: Generator generating result rows as `list`s
        """

    @overload
    def fetch_all_list(self, cls: Type[ListT], /, *args: Any, **kwargs: Any) -> AsyncGenerator[ListT, None]:
        """
        Generate lists of type cls for each row of the result set, with optionally passing args and kwargs to the
        cls constructor.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> class customlist(list):
        >>>     pass
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_list(customlist)
        >>>         print(row)
        >>>         print(type(row))
        >>>     # Output:
        >>>     # [1, "Product 1"]
        >>>     # <class 'customlist'>
        >>>     # [2, "Product 2"]
        >>>     # <class 'customlist'>
        >>>
        >>> asyncio.run(main())

        :param cls: List type that should be returned.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as lists of specified type.
        """

    async def fetch_all_list(
        self, cls: Type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any
    ) -> AsyncGenerator[ListT | list[Any], None]:
        """
        Generate lists of type cls for each row of the result set, with optionally passing args and kwargs to the
        cls constructor.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id`, `name` FROM `products`")
        >>>
        >>>     async for row in q.fetch_all_list()
        >>>         print(row)
        >>>     # Output:
        >>>     # [1, "Product 1"]
        >>>     # [2, "Product 2"]
        >>>
        >>> asyncio.run(main())

        :param cls: List type that should be returned. Defaults to plain `list`.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as lists of specified type.
        """
        async for row in self._fetch_all(self.fetch_list, cls, *args, **kwargs):
            yield cast(ListT, row)

    @overload
    async def fetch_all_scalar(self) -> list[Any]:
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id` FROM `products`")
        >>>
        >>>     ids = await q.fetch_all_scalar()
        >>>     print(ids)
        >>>     # [1, 2, 3, ...]
        >>>
        >>> asyncio.run(main())
        """

    @overload
    async def fetch_all_scalar(self, factory: Type[SetT]) -> SetT:
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id` FROM `products`")
        >>>
        >>>     ids = await q.fetch_all_scalar(set)
        >>>     print(ids)
        >>>     # {1, 2, 3, ...}
        >>>
        >>> asyncio.run(main())

        :param factory: Factory to use for the output type.
        """

    @overload
    async def fetch_all_scalar(self, factory: Type[ListT]) -> ListT:
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> class customlist(list):
        >>>     pass
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id` FROM `products`")
        >>>
        >>>     ids = await q.fetch_all_scalar(customlist)
        >>>     print(ids)
        >>>     # [1, 2, 3, ...]
        >>>     print(type(ids))
        >>>     # <class 'customlist'>
        >>>
        >>> asyncio.run(main())

        :param factory: Factory to use for the output type.
        """

    async def fetch_all_scalar(self, factory: Type[SetT | ListT | list[Any]] = list) -> SetT | ListT | list[Any]:
        """
        Fetch all rows from the result set as scalars.

        Example:

        >>> import asyncio
        >>> from asyncdb import Transaction, Result
        >>>
        >>> async def main():
        >>>     trx: Transaction = ...
        >>>     q: Result = await trx.query("SELECT `id` FROM `products`")
        >>>
        >>>     ids = await q.fetch_all_scalar()
        >>>     print(ids)
        >>>     # [1, 2, 3, ...]
        >>>
        >>> asyncio.run(main())

        :param factory: Factory to use for the output type. Can be MutableSet or MutableSequence. Defaults to `list`.
        """
        if not inspect.isclass(factory) or not issubclass(factory, (MutableSet, MutableSequence)):
            raise AttributeError("Unknown type factory. Can be only MutableSet or MutableSequence.")

        out = factory()
        append = cast(ListT, out).append if issubclass(factory, MutableSequence) else cast(SetT, out).add

        async for row in self.fetch_all_list():
            append(row[0])

        return cast(SetT | ListT, out)

    @abstractmethod
    def __bool__(self) -> bool:
        """
        Returns True if result is valid and contains rows to be fetched.
        """

    @property
    @abstractmethod
    def remote_app(self) -> str:
        """Returns remote_app for the result to enhance errors with correct identifier."""
        return ""

    async def close(self) -> None:  # noqa: B027
        """
        Cleanup the result set. This is called automatically by transaction, you don't need to call it manually.
        """


class BoolResult(Result, ABC):
    """
    Result of query that does not return any result set, just success of the operation. On this result, you cannot
    perform any fetch operations, just compare it to bool.

    Example:

    >>> import asyncio
    >>> from asyncdb import Transaction, BoolResult
    >>>
    >>> trx: Transaction = ...
    >>>
    >>> async def main():
    >>>     res = await trx.query("INSERT INTO `products` (`name`) VALUES ('Product 1')")
    >>>     print(type(res))
    >>>     # <class 'BoolResult'>
    >>>     print(bool(res))
    >>>     # True
    """

    def __init__(self, *args: Any, bool_result: bool, **kwargs: Any) -> None:
        """
        The `BoolResult` is created as a response to calling `Transaction.query()` automatically for queries that does not
        return result set. You should not need to instantiate it manually.

        :param args: Arguments passed to the super class constructor.
        :param bool_result: Actual bool result of the query.
        :param kwargs: Keyword arguments passed to the super class constructor.
        """
        super().__init__(*args, **kwargs)
        self._bool_result = bool_result

    @overload
    async def fetch_list(self, /) -> Optional[list[Any]]: ...

    @overload
    async def fetch_list(self, cls: Type[ListT], /, *args: Any, **kwargs: Any) -> Optional[ListT]: ...

    async def fetch_list(self, cls: Type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any) -> Optional[ListT | list[Any]]:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    @overload
    async def fetch_dict(self, /) -> Optional[dict[str, Any]]: ...

    @overload
    async def fetch_dict(self, cls: Type[DictT], /, *args: Any, as_kwargs: bool = False, **kwargs: Any) -> Optional[DictT]: ...

    async def fetch_dict(
        self, cls: Type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> Optional[DictT | dict[str, Any]]:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    @overload
    async def fetch_object(self, /) -> Optional[ResultRow]: ...

    @overload
    async def fetch_object(self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, **kwargs: Any) -> Optional[OBJ]: ...

    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow, /, *args: Any, **kwargs: Any
    ) -> Optional[OBJ | ResultRow]:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    def __bool__(self) -> bool:
        """
        Returns boolean result of the executed query.
        """
        return self._bool_result


class Transaction(ABC, Generic[ConnectionTypeT]):
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

    def __init__(self, connection: ConnectionTypeT) -> None:
        """
        :param connection: Connection this transaction should use.
        """
        self._connection = connection

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
        self, exc_type: Optional[Type[BaseException]], exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]
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

    @abstractmethod
    async def query(self, query: str, *args: Any, **kwargs: Any) -> Result:
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

    @abstractmethod
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
        query: str,
        args: Optional[Iterable[Any]],
        cls: Type[T] | Callable[..., T],
        *cls_args: Any,
        **cls_kwargs: Any,
    ) -> T:
        with _SL():
            res = await self.query(query, *(args or []))
            if not isinstance(res, Result) or isinstance(res, BoolResult):
                raise LogicError(query, "Query did not return result.")

            row = await result_fetcher(res)(cls, *cls_args, **cls_kwargs)
            if row is None:
                raise EntityNotFound(query, "Requested entity was not found.")

            another = await result_fetcher(res)(cls, *cls_args, **cls_kwargs)
            if another is not None:
                raise TooManyRowsError(query, "Query returned more than one row.")

            return row

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
        query: str,
        args: Optional[Iterable[Any]] = None,
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
    async def get_dict(self, query: str, args: Optional[Iterable[Any]] = None, /) -> dict[str, Any]: ...

    @overload
    async def get_dict(
        self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[DictT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> DictT: ...

    async def get_dict(
        self,
        query: str,
        args: Optional[Iterable[Any]] = None,
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
        with _SL():
            return await self._get(lambda res: res.fetch_dict, query, args, cls, *cls_args, **cls_kwargs)

    @overload
    async def get_list(self, query: str, args: Optional[Iterable[Any]] = None, /) -> list[Any]: ...

    @overload
    async def get_list(
        self, query: str, args: Optional[Iterable[Any]] = None, cls: Type[ListT] = ..., /, *cls_args: Any, **cls_kwargs: Any
    ) -> ListT: ...

    async def get_list(
        self,
        query: str,
        args: Optional[Iterable[Any]] = None,
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
        with _SL():
            return await self._get(lambda res: res.fetch_list, query, args, cls, *cls_args, **cls_kwargs)

    async def get_scalar(self, query: str, args: Optional[Iterable[Any]] = None) -> Any:
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
        with _SL():
            row: Sequence[Any] = await self.get_list(query, args)
            if len(row) != 1:
                raise TooManyRowsError(query, "Result must contain only one column to be fetched as scalar.")

            return row[0]

    @property
    @abstractmethod
    def connection(self) -> ConnectionTypeT:
        """
        Return connection that this transaction is open on.
        :return: connection object
        """

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Cleanup the connection associated with this transaction and prepare it for usage by next transaction.
        """


@deprecated("Use Result.fetch_all_object directly.")
class ObjectFetcher(Generic[ResObjT]):
    """
    Helper class that acts as generator that fetches objects. The only difference between `Result.fetch_all_object()` is,
    that this can be type-safe parametrized with actual type of object that it should generate, which helps with
    code completion and mypy checks.

    This should not be needed anymore, but is there for backward compatibility. Currently, `Result.fetch_object()` should be
    correctly typed for Python 3.11+ to provide correct type hints even without this helper class.

    Example:

    >>> import asyncio
    >>> from asyncdb import ObjectFetcher
    >>> from asyncdb.aiomysql import Transaction, AioMySQLConnection
    >>> from dataclasses import dataclass
    >>>
    >>> connection: AioMySQLConnection = ...
    >>>
    >>> @dataclass
    >>> class Product:
    >>>     id: int
    >>>     name: str
    >>>
    >>> async def fetch_products() -> list[Product]:
    >>>     out: list[Product] = []
    >>>     async with Transaction(connection) as trx:
    >>>         q = await trx.query("SELECT `id`, `name` FROM `products`")
    >>>         async for product in ObjectFetcher[Product](q, Product):
    >>>             out.append(product)
    >>>     return out
    >>>
    >>> asyncio.run(fetch_products())
    """

    def __init__(
        self,
        q: Result,
        cls: Type[ResultRow | ResObjT] | Callable[..., ResObjT] = ResultRow,
        cls_args: Optional[Iterable[Any]] = None,
        cls_kwargs: Optional[dict[str, Any]] = None,
        as_kwargs: bool = True,
    ):
        """
        Construct the `ObjectFetcher`.

        :param q: Query result
        :param cls: See `Result.fetch_object()` for details.
        :param cls_args: See `Result.fetch_object()` for details.
        :param cls_kwargs: See `Result.fetch_object()` for details.
        :param as_kwargs: See `Result.fetch_object()` for details.
        """
        self.q: Result = q
        """Result this ObjectFetcher will iterate over."""

        self.cls = cls
        """Class (or factory) representing the object that should be returned."""

        self.cls_args = cls_args or []
        """Optional arguments to the cls constructor."""

        self.cls_kwargs = cls_kwargs or {}
        """Optional kwargs for the constructor."""

        self.as_kwargs = as_kwargs
        """
        Should the row attributes be passed as kwargs to the constructor? If False, properties will be set directly using
        setattr().
        """

    def __aiter__(self) -> Self:
        """
        Beginning of the iteration.
        """
        return self

    async def __anext__(self) -> ResObjT:
        """
        Return one row from the result set as object of specified type.
        """
        resp = cast(ResObjT, await self.q.fetch_object(self.cls, *self.cls_args, as_kwargs=self.as_kwargs, **self.cls_kwargs))
        if resp is None:
            raise StopAsyncIteration()

        return resp
