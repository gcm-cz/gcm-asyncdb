from __future__ import annotations

import inspect
from collections.abc import Callable, MutableMapping, MutableSequence, MutableSet
from dataclasses import is_dataclass
from typing import TYPE_CHECKING, Any

from typing_extensions import TypeVar

from asyncdb._pydantic_compat import HAVE_PYDANTIC, BaseModel

if TYPE_CHECKING:
    from asyncdb.asyncdb.connection import ConnectionProtocol
    from asyncdb.asyncdb.transaction import Transaction
    from asyncdb.pool import ConnectionPool, DatabaseConnectionPool

T = TypeVar("T")

OBJ = TypeVar("OBJ", bound=object)
DICT = MutableMapping[str, Any]
LST = MutableSequence[Any]
SET = MutableSet[Any]
FETCH = TypeVar("FETCH")
OBJ_OR_FACTORY = type[OBJ] | Callable[..., OBJ]

ArgsT = TypeVar("ArgsT", bound=tuple[Any, ...])
DictT = TypeVar("DictT", bound=DICT)
ListT = TypeVar("ListT", bound=LST)
SetT = TypeVar("SetT", bound=SET)


class ResultRow:
    """
    Default class for returning objects from SQL result.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.__dict__.update(kwargs)

    def __getattr__(self, item: str) -> Any:
        return self.__dict__.get(item)

    def __setattr__(self, item: str, value: Any) -> None:
        self.__dict__[item] = value

    def __repr__(self) -> str:
        items = ", ".join([f"{key}={val!r}" for key, val in self.__dict__.items()])
        return f"<ResultRow {items}>"


ResObjT = TypeVar("ResObjT", default=ResultRow, bound=object)
ConnectionTypeT = TypeVar("ConnectionTypeT", bound="ConnectionProtocol", default=Any)
TransactionTypeT = TypeVar("TransactionTypeT", bound="Transaction", default=Any)
ConnectionPoolT = TypeVar("ConnectionPoolT", bound="ConnectionPool")
DatabaseConnectionPoolT = TypeVar("DatabaseConnectionPoolT", bound="DatabaseConnectionPool")


def _accepts_kwargs(cls: Any) -> bool:
    """
    Utility function to determine value for `as_kwargs` parameter if `fetch_object` method of transaction.

    Currently, it returns True for dataclasses and Pydantic models, if pydantic is available.

    :param cls: Class to check.
    :return: True if class constructor accepts kwargs, False otherwise.
    """

    if not inspect.isclass(cls):
        return False

    if is_dataclass(cls) or (HAVE_PYDANTIC and issubclass(cls, BaseModel)):
        return True

    return False
