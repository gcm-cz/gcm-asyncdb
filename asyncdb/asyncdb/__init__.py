"""
Generic base classes and interfaces for asyncdb drivers. Documentation in this module covers generic usage without any
driver-specific details.

For driver-specific documentation and examples, please consult respective driver's module documentation.
"""

from asyncdb.asyncdb.bool_result import BoolResult
from asyncdb.asyncdb.connection import ConnectionProtocol
from asyncdb.asyncdb.object_fetcher import ObjectFetcher
from asyncdb.asyncdb.observer import (
    GenericQueryObserver,
    GenericTransactionObserver,
    QueryObserver,
    TransactionObserver,
)
from asyncdb.asyncdb.result import Result
from asyncdb.asyncdb.transaction import Transaction

__all__ = [
    "BoolResult",
    "ConnectionProtocol",
    "GenericQueryObserver",
    "GenericTransactionObserver",
    "ObjectFetcher",
    "QueryObserver",
    "Result",
    "Transaction",
    "TransactionObserver",
]
