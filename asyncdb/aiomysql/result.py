from __future__ import annotations

import datetime
from collections.abc import MutableSequence, MutableSet
from copy import copy
from typing import TYPE_CHECKING, Any, Optional, Type

import aiomysql

from ..generics import OBJ, OBJ_OR_FACTORY, DICT, LST
from ..exceptions import LogicError
from ..asyncdb import Result as AsyncResultBase, ResultRow

if TYPE_CHECKING:
    from .transaction import Transaction


class Result(AsyncResultBase):
    """
    Result of query.
    """

    def __init__(self, transaction: Transaction, cursor: aiomysql.Cursor, query: str):
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

        # Consume all rows in current result set.
        while await self.cursor.fetchone():
            pass

        # Consume all remaining result sets.
        while await self.cursor.nextset():
            while await self.cursor.fetchone():
                pass

        await self.transaction._clean_cursor(self)
        self._closed = True

    async def _fetch_raw(self):
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

    # pylint: disable=keyword-arg-before-vararg
    async def fetch_list(self, cls: Type[LST] = list, *args, **kwargs) -> Optional[LST]:
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

    # pylint: disable=keyword-arg-before-vararg
    async def fetch_dict(self, cls: Type[DICT] = dict, *args, **kwargs) -> Optional[DICT]:
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
        for idx, column in enumerate(self.cursor.description):
            result[column[0]] = self._process_value(row[idx])

        return result

    # pylint: disable=keyword-arg-before-vararg
    async def fetch_object(self, cls: OBJ_OR_FACTORY = ResultRow, *args, as_kwargs: bool = True, **kwargs
                           ) -> Optional[OBJ]:
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
            for idx, column in enumerate(self.cursor.description):
                my_kwargs[column[0]] = self._process_value(row[idx])

        result = cls(*args, **my_kwargs)

        if not as_kwargs:
            for idx, column in enumerate(self.cursor.description):
                setattr(result, column[0], self._process_value(row[idx]))

        return result

    async def fetch_all_scalar(self, factory: Type[MutableSet | MutableSequence] = list) -> list[Any]:
        """
        Yields values from single-column queries.
        """
        if len(self.cursor.description) != 1:
            raise LogicError(self._query, "Query returned more than one column.")

        return await super().fetch_all_scalar(factory)
