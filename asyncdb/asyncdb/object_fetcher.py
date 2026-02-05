from collections.abc import Callable, Iterable
from typing import Any, Generic, Self, cast

from typing_extensions import deprecated

from asyncdb.asyncdb.result import Result
from asyncdb.generics import ResObjT, ResultRow


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
    >>> from asyncdb.asyncdb import Transaction, ConnectionProtocol
    >>> from dataclasses import dataclass
    >>>
    >>> connection: ConnectionProtocol = ...
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
        cls: type[ResultRow | ResObjT] | Callable[..., ResObjT] = ResultRow,
        cls_args: Iterable[Any] | None = None,
        cls_kwargs: dict[str, Any] | None = None,
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
