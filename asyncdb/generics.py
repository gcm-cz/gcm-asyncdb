from typing import TypeVar, ParamSpec, Dict, Any, List, Union, Type, Callable

T = TypeVar('T')
P = ParamSpec('P')
OBJ = TypeVar('OBJ')
DICT = Dict[str, Any]
LST = List[Any]
FETCH = TypeVar('FETCH')
OBJ_OR_FACTORY = Union[Type[OBJ], Callable[..., OBJ]]  # pylint: disable=invalid-name
