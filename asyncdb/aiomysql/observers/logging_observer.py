"""Module to provide backward compatibility for observers before generalizing them in asyncdb.asyncdb.observer."""

import warnings
from typing import Any

from asyncdb.asyncdb.observers import logging_observer as _observers

# pylint: disable=undefined-all-variable
# ruff: disable[F822]
__all__ = [
    "LoggingObserver",
]
# ruff: enable[F822]


def __getattr__(name: str) -> Any:
    if name in __all__:
        warnings.warn(
            f"{__name__}:{name} has been moved to asyncdb.asyncdb.observers.logging_observer.{name}.",
            DeprecationWarning,
            stacklevel=2,
        )
        return getattr(_observers, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
