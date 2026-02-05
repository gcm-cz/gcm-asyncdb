from contextlib import AbstractContextManager
from typing import Any

from opentelemetry.trace import Span, SpanKind, StatusCode, Tracer

from asyncdb import Transaction
from asyncdb.asyncdb.connection import ConnectionProtocol
from asyncdb.asyncdb.observer import GenericTransactionObserver
from asyncdb.exceptions import Error


class OpenTelemetryTransactionObserver(
    GenericTransactionObserver[tuple[AbstractContextManager[Span], Span], tuple[AbstractContextManager[Span], Span]]
):
    """OpenTelemetry observer for transactions and queries."""

    def __init__(self, tracer: Tracer) -> None:
        """
        :param tracer: OpenTelemetry tracer to create spans with.
        """
        self.tracer = tracer

    def observe_transaction_before(self, trx: Transaction, **kwargs: Any) -> tuple[AbstractContextManager[Span], Span]:
        """Start a span for the transaction."""
        attributes: dict[str, Any] = {"db.namespace": trx.connection.db}

        if trx.connection.host:
            attributes["server.address"] = trx.connection.host

        if trx.connection.port:
            attributes["server.port"] = trx.connection.port

        context = self.tracer.start_as_current_span(
            name="sql_transaction",
            kind=SpanKind.CLIENT,
            attributes=attributes,
        )
        return context, context.__enter__()  # pylint: disable=unnecessary-dunder-call

    def observe_transaction_commit(
        self, trx: Transaction, context: tuple[AbstractContextManager[Span], Span], **kwargs: Any
    ) -> None:
        """End the transaction span."""
        _, span = context
        span.set_status(StatusCode.OK, "COMMIT")

    def observe_transaction_rollback(
        self, trx: Transaction, context: tuple[AbstractContextManager[Span], Span], **kwargs: Any
    ) -> None:
        """End the transaction span with error status."""
        _, span = context
        span.set_status(StatusCode.OK, "ROLLBACK")

    def observe_transaction_end(
        self, trx: Transaction, context: tuple[AbstractContextManager[Span], Span], **kwargs: Any
    ) -> None:
        """End the transaction span."""
        ctx, _ = context
        ctx.__exit__(None, None, None)

    def observe_query_before(
        self,
        conn: ConnectionProtocol,
        query: str,
        trx_context: tuple[AbstractContextManager[Span], Span],
        *,
        query_pattern: str,
        **kwargs: Any,
    ) -> tuple[AbstractContextManager[Span], Span]:
        """Add query information to the transaction span."""
        query_type = query_pattern.lstrip().split(" ", 1)[0].lower()

        context = self.tracer.start_as_current_span(
            name=f"sql_query_{query_type}",
            kind=SpanKind.CLIENT,
            attributes={
                "db.query.text": query_pattern,
            },
        )
        return context, context.__enter__()  # pylint: disable=unnecessary-dunder-call

    def observe_query_after(
        self,
        conn: ConnectionProtocol,
        query: str,
        context: tuple[AbstractContextManager[Span], Span],
        trx_context: tuple[AbstractContextManager[Span], Span],
        **kwargs: Any,
    ) -> None:
        """End the query span."""
        ctx, span = context
        span.set_status(StatusCode.OK)
        ctx.__exit__(None, None, None)

    def observe_query_error(
        self,
        conn: ConnectionProtocol,
        query: str,
        error: BaseException,
        context: tuple[AbstractContextManager[Span], Span],
        trx_context: tuple[AbstractContextManager[Span], Span],
        **kwargs: Any,
    ) -> None:
        """End the query span with error status."""
        ctx, span = context
        if isinstance(error, Error):
            span.set_attribute("db.response.status_code", error.code)

        span.set_attributes(
            {
                "error.type": type(error).__name__,
            }
        )

        span.record_exception(error)
        span.set_status(StatusCode.ERROR, str(error))
        ctx.__exit__(None, None, None)
