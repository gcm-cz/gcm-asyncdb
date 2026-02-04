from __future__ import annotations

from collections.abc import Collection
from logging import Logger
from typing import TYPE_CHECKING, Any, Protocol, Self

if TYPE_CHECKING:
    from asyncdb.asyncdb.observer import GenericQueryObserver


class ConnectionProtocol(Protocol):
    """
    Protocol describing required methods and properties of a database connection class.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    @property
    def logger(self) -> Logger:
        """
        Get the logger associated with the connection.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def remote_app(self) -> str:
        """
        Get the remote application name associated with the connection.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def host(self) -> str | None:
        """
        Get the host of the database server. If the connection does not use a network connection, return None.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def port(self) -> int | None:
        """
        Get the port of the database server. If the connection does not use a network connection, return None.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def user(self) -> str | None:
        """
        Get the user name used for the connection. If the connection is not authenticated, return None.
        """
        raise NotImplementedError  # pragma: no cover

    @property
    def db(self) -> str | None:
        """
        Get the database name used for the connection.
        """
        raise NotImplementedError  # pragma: no cover

    def attach(self, observer: GenericQueryObserver[Any, Self]) -> None:
        """
        Attach an observer to the connection.

        :param observer: Observer to attach.
        """
        raise NotImplementedError  # pragma: no cover

    def detach(self, observer: GenericQueryObserver[Any, Self]) -> None:
        """
        Detach an observer from the connection.

        :param observer: Observer to detach.
        """
        raise NotImplementedError  # pragma: no cover

    def observe_query_before(self, sql: str, *, query_pattern: str, query_args: Collection[Any], **kwargs: Any) -> dict[int, Any]:
        """
        Callback called before query execution.
        :param sql: SQL query
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer
        :return: Results from the observer callbacks, that are passed to the after and error callbacks.
        """
        raise NotImplementedError  # pragma: no cover

    def observe_query_after(
        self,
        sql: str,
        observer_context: dict[int, Any],
        *,
        query_pattern: str,
        query_args: Collection[Any],
        **kwargs: Any,
    ) -> None:
        """
        Callback called after query execution.
        :param sql: SQL query
        :param observer_context: Context from the before callback
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        raise NotImplementedError  # pragma: no cover

    def observe_query_error(
        self,
        sql: str,
        exc: BaseException,
        observer_context: dict[int, Any],
        *,
        query_pattern: str,
        query_args: Collection[Any],
        **kwargs: Any,
    ) -> None:
        """
        Callback called when query execution raises an exception.
        :param sql: SQL query
        :param exc: Exception raised
        :param observer_context: Context from the before callback
        :param query_pattern: Original query pattern used for the execution
        :param query_args: Query arguments used for the execution
        :param kwargs: Additional kwargs passed to the observer while executing the query.
        """
        raise NotImplementedError  # pragma: no cover
