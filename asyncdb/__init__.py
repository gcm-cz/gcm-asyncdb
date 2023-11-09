"""
Wrappers around various database driver with support for async operations.
"""

from logging import getLogger

from .exceptions import TooManyRowsError, LogicError, EntityNotFound
from .asyncdb import Transaction, Result, ObjectFetcher, ResultRow
from .context import TransactionContext

# Base logger for the database subsystem.
logger = getLogger("db")
