"""
Database-related exceptions.
"""

from __future__ import annotations

from typing import Optional


class Error(Exception):
    """
    Base error class for database exceptions.
    """

    def __init__(self, message: str, code: Optional[int] = None, remote_app: str = "") -> None:
        """
        :param message: Error message
        :param code: Optional error code (MySQL often returns error codes for its errors).
        :param remote_app: remote_app from the connection that caused the error, if provided in the configuration of the pool.
        """

        super().__init__(message)
        self.code = code
        self.remote_app = remote_app


class ConnectError(Error):
    """
    Connection related errors.
    """


class QueryError(Error):
    """
    Errors based on query execution.
    """

    def __init__(self, query: str, message: str, code: Optional[int] = None, remote_app: str = "") -> None:
        """
        :param query: Query that caused the error.
        :param message: Error message describing the error.
        :param code: Error code from MySQL server, if available.
        :param remote_app: remote_app from the connection that caused the error, if provided in the configuration of the pool.
        """

        super().__init__(message, code, remote_app=remote_app)
        self.query = query


class LogicError(QueryError):
    """
    Logical error
    """


class TooManyRowsError(LogicError):
    """
    Too many rows returned where expected only one row.
    """


class IntegrityError(LogicError):
    """
    Data integrity error such as foreign key violation or duplicate key.
    """


class EntityNotFound(LogicError):
    """
    Requested entity was not found.
    """

    def __init__(self, query: str, message: str = "Entity not found.", code: Optional[int] = None, remote_app: str = "") -> None:
        """
        :param query: Query that caused the error.
        :param message: Error message describing the error.
        :param code: Error code from MySQL server, if available.
        :param remote_app: remote_app from the connection that caused the error, if provided in the configuration of the pool.
        """
        super().__init__(query, message, code, remote_app=remote_app)


class DuplicateEntry(IntegrityError):
    """
    Duplicate entry exception.
    """

    def __init__(self, query: str, message: str = "Duplicate entry.", code: Optional[int] = None, remote_app: str = "") -> None:
        """
        :param query: Query that caused the error.
        :param message: Error message describing the error.
        :param code: Error code from MySQL server, if available.
        :param remote_app: remote_app from the connection that caused the error, if provided in the configuration of the pool.
        """
        super().__init__(query, message, code, remote_app=remote_app)


class ParseError(QueryError):
    """
    Query parse error
    """


class SyntaxError(QueryError):  # pylint: disable=redefined-builtin  # noqa: A001
    """
    Query syntax error
    """


class Deadlock(QueryError):
    """
    Deadlock occured in DB. Should retry.
    """


class LostConnection(QueryError):
    """
    Lost connection to MySQL during query.
    """


class LockWaitTimeout(QueryError):
    """
    Lock wait timeout while waiting for release lock.
    """


class ServerGone(QueryError):
    """
    MySQL server has gone away.
    """


class QueryTooBig(QueryError):
    """
    Packet too large.
    """


class ImplicitRollback(RuntimeWarning):
    """
    Rolling back uncommited transaction.
    """


class UnableToCreateConnection(RuntimeError):
    """
    Exception thrown when connection cannot be created. It might be caused by connection failing to establish or
    initial check after establishing the connection fails.
    """


class ScrollIndexError(QueryError, IndexError):
    """
    Index error while seeking.
    """
