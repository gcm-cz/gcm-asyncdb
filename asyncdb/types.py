from abc import ABC
from typing import TYPE_CHECKING, Generic, TypeVar

try:
    from wrapt import BaseObjectProxy
except ImportError:
    from wrapt import ObjectProxy as BaseObjectProxy

T = TypeVar("T")

# As of wrapt 2.0.1, there is an error that occurs when using generics with ObjectProxy in runtime. It states that
# ObjectProxy type is not subscriptable. On the other hand, when type checking, mypy expects the type argument for the
# ObjectProxy to be passed. So this is a compatibility workaround to satisfy both mypy and runtime.

if TYPE_CHECKING:

    class ObjectProxy(ABC, Generic[T], BaseObjectProxy[T]):
        """A generic ObjectProxy for type checking purposes."""

else:

    class ObjectProxy(ABC, Generic[T], BaseObjectProxy):
        """A generic ObjectProxy for runtime purposes."""
