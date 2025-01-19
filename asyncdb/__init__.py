"""
Wrappers around various database driver with support for async operations.
"""

from logging import getLogger

from .asyncdb import ObjectFetcher, Result, Transaction
from .context import TransactionContext
from .exceptions import EntityNotFound, LogicError, TooManyRowsError
from .generics import ResultRow

# Base logger for the database subsystem.
logger = getLogger("db")


__all__ = [
    "EntityNotFound",
    "LogicError",
    "ObjectFetcher",
    "Result",
    "ResultRow",
    "TooManyRowsError",
    "Transaction",
    "TransactionContext",
    "logger",
]
