from __future__ import annotations

import datetime
from copy import copy
from typing import TYPE_CHECKING, Any, Optional, Type, overload

from ..asyncdb import BoolResult as BoolResultBase
from ..asyncdb import Result as AsyncResultBase
from ..exceptions import LogicError
from ..generics import OBJ, OBJ_OR_FACTORY, DictT, ListT, ResultRow, SetT
from .cursor import Cursor

if TYPE_CHECKING:
    from .transaction import Transaction


class Result(AsyncResultBase):
    """
    Result of query.
    """

    def __init__(self, transaction: Transaction, cursor: Optional[Cursor], query: str) -> None:
        self.transaction = transaction
        self.cursor = cursor
        self._closed = False
        self._query = query

    async def close(self) -> None:
        """
        Cleanup the result when used. Reads and discards all remaining rows from all remaining result sets.
        """
        if self._closed:
            return

        if self.cursor:
            # Consume all rows in current result set.
            while await self.cursor.fetchone():
                pass

            # Consume all remaining result sets.
            while await self.cursor.nextset():
                while await self.cursor.fetchone():
                    pass

            # pylint: disable=protected-access
            await self.transaction._clean_cursor(self)

        self._closed = True

    async def _fetch_raw(self) -> Optional[Any]:
        if not self.cursor:
            return None

        row = await self.cursor.fetchone()
        if not row and await self.cursor.nextset():
            row = await self.cursor.fetchone()

        if not row:
            await self.close()
            return None

        return row

    @staticmethod
    def _process_value(value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return value.replace(tzinfo=datetime.timezone.utc)

        return value

    @overload
    async def fetch_list(self, /) -> Optional[list[Any]]: ...

    @overload
    async def fetch_list(self, cls: Type[ListT], /, *args: Any, **kwargs: Any) -> Optional[ListT]: ...

    async def fetch_list(self, cls: Type[ListT | list[Any]] = list, /, *args: Any, **kwargs: Any) -> Optional[ListT | list[Any]]:
        """
        Fetch one row from result as list. The list type can be specified as the cls argument.
        :param cls: List type that should be returned. Defaults to plain list.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs to the cls constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """
        row = await self._fetch_raw()
        if row is None:
            return None

        result = cls(*args, **kwargs)
        for column in row:
            result.append(self._process_value(column))

        return result

    @overload
    async def fetch_dict(self, /) -> Optional[dict[str, Any]]:
        """
        Fetch one row from result as dictionary. The dictionary type can be specified as the cls argument.
        :return: dict containing mapping from column names to values or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_dict(self, cls: Type[DictT], /, *args: Any, **kwargs: Any) -> Optional[DictT]:
        """
        Fetch one row from result as dictionary. The dictionary type can be specified as the cls argument.
        :param cls: Dictionary type that should be returned.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    async def fetch_dict(
        self, cls: Type[DictT | dict[str, Any]] = dict, /, *args: Any, **kwargs: Any
    ) -> Optional[DictT | dict[str, Any]]:
        """
        Fetch one row from result as dictionary. The dictionary type can be specified as the cls argument.
        :param cls: Dictionary type that should be returned. Defaults to plain dict.
        :param args: Optional arguments to the cls constructor.
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """
        row = await self._fetch_raw()
        if row is None:
            return None

        result = cls(*args, **kwargs)
        for idx, column in enumerate(self.description):
            result[column[0]] = self._process_value(row[idx])

        return result

    @overload
    async def fetch_object(self, /) -> Optional[ResultRow]:
        """
        Fetch one row from result as object of type ResultRow.
        :return: Instance of ResultRow created from result row or None if no rows are remaining in the result set.
        """

    @overload
    async def fetch_object(self, cls: OBJ_OR_FACTORY[OBJ], /, *args: Any, as_kwargs: bool = True, **kwargs: Any) -> Optional[OBJ]:
        """
        Fetch one row from result as object of type specified by the cls argument.
        :param cls: Class representing the object that should be returned or factory making one.
        :param args: Arguments to the cls constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """

    async def fetch_object(
        self, cls: OBJ_OR_FACTORY[OBJ] | Type[ResultRow] = ResultRow, /, *args: Any, as_kwargs: bool = True, **kwargs: Any
    ) -> Optional[OBJ | ResultRow]:
        """
        Fetch one row from result as object of type specified by the cls argument.
        :param cls: Class (or factory) representing the object that should be returned.
        :param args: Arguments to the cls constructor.
        :param as_kwargs: Should the row attributes be passed as kwargs to the constructor? If False, properties
          will be set directly using setattr().
        :param kwargs: Optional kwargs for the constructor.
        :return: Instance of cls created from result row or None if no rows are remaining in the result set.
        """
        row = await self._fetch_raw()
        if row is None:
            return None

        my_kwargs = copy(kwargs)

        if as_kwargs:
            for idx, column in enumerate(self.description):
                my_kwargs[column[0]] = self._process_value(row[idx])

        result = cls(*args, **my_kwargs)

        if not as_kwargs:
            for idx, column in enumerate(self.description):
                setattr(result, column[0], self._process_value(row[idx]))

        return result

    @overload
    async def fetch_all_scalar(self) -> list[Any]:
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.
        """

    @overload
    async def fetch_all_scalar(self, factory: Type[SetT]) -> SetT:  # pylint: disable=arguments-differ
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.
        :param factory: Factory to use for the output type.
        """

    @overload
    async def fetch_all_scalar(self, factory: Type[ListT]) -> ListT:  # pylint: disable=arguments-differ
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.
        :param factory: Factory to use for the output type.
        """

    async def fetch_all_scalar(self, factory: Type[ListT | SetT | list[Any]] = list) -> ListT | SetT | list[Any]:
        """
        Fetch all rows from result as a list of scalars. Only for queries returning single column.
        """
        if len(self.description) != 1:
            raise LogicError(self._query, "Query returned more than one column.", remote_app=self.remote_app)

        return await super().fetch_all_scalar(factory)

    @property
    def description(self) -> list[tuple[str, ...]]:
        """
        Description of the columns in the result set.
        """
        if not self.cursor:
            return []

        return self.cursor.description

    @property
    def remote_app(self) -> str:
        """
        Remote app associated with the transaction to enhance error messages.
        """
        return self.transaction.remote_app

    def __bool__(self) -> bool:
        return True


class BoolResult(BoolResultBase, Result):
    """
    Returns boolean result of the query.
    """

    def __init__(self, transaction: Transaction, bool_result: bool, query: str) -> None:
        super().__init__(transaction, None, query, bool_result=bool_result)

    def __repr__(self) -> str:
        return f"<Result {self._bool_result}>"
