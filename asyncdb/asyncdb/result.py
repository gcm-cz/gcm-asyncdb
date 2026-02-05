import inspect
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Awaitable, Callable, MutableSequence, MutableSet
from typing import Any, cast, overload

from asyncdb.generics import OBJ, OBJ_OR_FACTORY, DictT, ListT, ResultRow, SetT, _accepts_kwargs


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
    async def fetch_list(self, /) -> list[Any] | None:
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
    async def fetch_list(self, cls: type[ListT], /, *args: Any, **kwargs: Any) -> ListT | None:
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
    async def fetch_list(self, cls: type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any) -> ListT | list[Any] | None:
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
    async def fetch_dict(self, /) -> dict[str, Any] | None:
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
    async def fetch_dict(self, cls: type[DictT], /, *args: Any, **kwargs: Any) -> DictT | None:
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
        self, cls: type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, **kwargs: Any
    ) -> DictT | dict[str, Any] | None:
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
    async def fetch_object(self, /) -> ResultRow | None:
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
    async def fetch_object(self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, as_kwargs: bool = False, **kwargs: Any) -> OBJ | None:
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
          will be set directly using setattr(). If not specified, defaults to True if cls is dataclass or pydantic model,
          False otherwise.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @abstractmethod
    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | type[ResultRow] = ResultRow, /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> OBJ | ResultRow | None:
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
          will be set directly using setattr(). If not specified, defaults to True if cls is dataclass or pydantic model,
          False otherwise.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    @staticmethod
    async def _fetch_all(
        method: Callable[[Any, Any, Any], Awaitable[Any]], cls: type[OBJ] | Callable[..., OBJ], *args: Any, **kwargs: Any
    ) -> AsyncGenerator[OBJ, None]:
        while True:
            row = await method(cls, *args, **kwargs)

            if row is None:
                break

            yield row

    @overload
    def fetch_all_object(self, /) -> AsyncGenerator[ResultRow, None]:
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
        self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, as_kwargs: bool | None = None, **kwargs: Any
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
          will be set directly using setattr(). If not specified, defaults to True if cls is dataclass or pydantic model,
          False otherwise.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as objects of specified type.
        """

    async def fetch_all_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | type[ResultRow] = ResultRow, /, *args: Any, as_kwargs: bool | None = None, **kwargs: Any
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
          will be set directly using setattr(). If not specified, defaults to True if cls is dataclass or pydantic model,
          False otherwise.
        :param kwargs: Optional kwargs for the constructor.
        :return: Generator generating result rows as objects of specified type.
        """
        if as_kwargs is None:
            as_kwargs = _accepts_kwargs(cls)

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
    def fetch_all_dict(self, cls: type[DictT], /, *args: Any, **kwargs: Any) -> AsyncGenerator[DictT, None]:
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
        self, cls: type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, **kwargs: Any
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
    def fetch_all_list(self, cls: type[ListT], /, *args: Any, **kwargs: Any) -> AsyncGenerator[ListT, None]:
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
        self, cls: type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any
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
    async def fetch_all_scalar(self, factory: type[SetT]) -> SetT:
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
    async def fetch_all_scalar(self, factory: type[ListT]) -> ListT:
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

    async def fetch_all_scalar(self, factory: type[SetT | ListT | list[Any]] = list) -> SetT | ListT | list[Any]:
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
