"""
Wrappers around various database driver with support for async operations.
"""

from logging import getLogger
from typing import TypeVar, Type, Callable, Any, Union, Dict, List, ParamSpec

from .exceptions import TooManyRowsError, LogicError, EntityNotFound


class ResultRow:
    """
    Default class for returning objects from SQL result.
    """

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


T = TypeVar('T')
P = ParamSpec('P')
OBJ = TypeVar('OBJ')
DICT = Dict[str, Any]
LST = List[Any]
FETCH = TypeVar('FETCH')
OBJ_OR_FACTORY = Union[Type[OBJ], Callable[..., OBJ]]  # pylint: disable=invalid-name


# Base logger for the database subsystem.
logger = getLogger("db")
