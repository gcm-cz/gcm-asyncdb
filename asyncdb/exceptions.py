"""
Database-related exceptions.
"""

from typing import Optional


class Error(Exception):
    """
    Base error class for database exceptions.
    """

    def __init__(self, message: str, code: Optional[int] = None, remote_app: str = ""):
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

    def __init__(self, query: str, message: str, code: Optional[int] = None, remote_app: str = ""):
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

    def __init__(self, query: str, message: str = "Entity not found.", code: Optional[int] = None, remote_app: str = ""):
        super().__init__(query, message, code, remote_app=remote_app)


class DuplicateEntry(IntegrityError):
    """
    Duplicate entry exception.
    """

    def __init__(self, query: str, message: str = "Duplicate entry.", code: Optional[int] = None, remote_app: str = ""):
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
