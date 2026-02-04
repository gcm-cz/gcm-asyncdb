from __future__ import annotations

from collections.abc import Callable, MutableMapping, MutableSequence, MutableSet
from typing import TYPE_CHECKING, Any

from typing_extensions import TypeVar

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
