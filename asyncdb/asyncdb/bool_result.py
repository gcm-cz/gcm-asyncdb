from abc import ABC
from typing import Any, overload

from asyncdb.asyncdb.result import Result
from asyncdb.exceptions import Error
from asyncdb.generics import OBJ, OBJ_OR_FACTORY, DictT, ListT, ResultRow


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
    async def fetch_list(self, /) -> list[Any] | None: ...

    @overload
    async def fetch_list(self, cls: type[ListT], /, *args: Any, **kwargs: Any) -> ListT | None: ...

    async def fetch_list(self, cls: type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any) -> ListT | list[Any] | None:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    @overload
    async def fetch_dict(self, /) -> dict[str, Any] | None: ...

    @overload
    async def fetch_dict(self, cls: type[DictT], /, *args: Any, as_kwargs: bool = False, **kwargs: Any) -> DictT | None: ...

    async def fetch_dict(
        self, cls: type[DictT | dict[str, Any]] = dict[str, Any], /, *args: Any, as_kwargs: bool = False, **kwargs: Any
    ) -> DictT | dict[str, Any] | None:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    @overload
    async def fetch_object(self, /) -> ResultRow | None: ...

    @overload
    async def fetch_object(self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, **kwargs: Any) -> OBJ | None: ...

    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | type[ResultRow] = ResultRow, /, *args: Any, **kwargs: Any
    ) -> OBJ | ResultRow | None:
        """
        On BoolResult, you cannot fetch any rows, so this method always raises an error.
        """
        raise Error("Cannot fetch list from query that did not return result set.", remote_app=self.remote_app)

    def __bool__(self) -> bool:
        """
        Returns boolean result of the executed query.
        """
        return self._bool_result

    def __eq__(self, other: Any) -> bool:
        """
        Compare the result with another object. If the other object is a bool, it will compare the bool result of the query
        with the other object.
        """

        if isinstance(other, bool):
            return bool(self) == other

        if isinstance(other, BoolResult):
            return bool(self) == bool(other)

        return NotImplemented
