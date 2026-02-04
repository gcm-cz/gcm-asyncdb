from typing import Any

from opentracing import Scope, Tracer
from opentracing.ext import tags

from asyncdb import Transaction
from asyncdb.asyncdb.connection import ConnectionProtocol
from asyncdb.asyncdb.observer import GenericTransactionObserver


class OpentracingObserver(GenericTransactionObserver[Scope, Scope]):
    """
    OpenTracing support for asyncdb.

    Note that opentracing is officially deprecated in favor of OpenTelemetry. However, there are still many systems using it,
    so the support is there.
    """

    def __init__(self, tracer: Tracer) -> None:
        self.tracer = tracer

    def observe_transaction_before(self, trx: Transaction, **kwargs: Any) -> Scope:
        """Start a new transaction scope."""

        scope = self.tracer.start_active_span(
            "sql_transaction",
            child_of=self.tracer.active_span,
            tags={
                tags.SPAN_KIND: tags.SPAN_KIND_RPC_CLIENT,
                tags.DATABASE_TYPE: "sql",
                tags.PEER_SERVICE: trx.connection.remote_app,
                tags.DATABASE_INSTANCE: trx.connection.host,
                tags.DATABASE_USER: trx.connection.user,
            },
        )
        return scope

    def observe_transaction_end(self, trx: Transaction, context: Scope, **kwargs: Any) -> None:
        """Finish transaction scope."""

        context.close()

    def observe_query_before(
        self,
        conn: ConnectionProtocol,
        query: str,
        trx_context: Scope,
        *,
        query_pattern: str,
        **kwargs: Any,
    ) -> Scope:
        """Start a query scope before query execution."""

        query_type = query_pattern.lstrip().split(" ", 1)[0].lower()
        scope = self.tracer.start_active_span(
            f"sql_query_{query_type}",
            child_of=self.tracer.active_span,
            tags={"query": query_pattern},
        )
        return scope

    def observe_query_error(
        self, conn: ConnectionProtocol, query: str, error: BaseException, context: Scope, trx_context: Scope, **kwargs: Any
    ) -> None:
        """Set error to currently active span (the query span) and close the query scope."""

        context.span.set_tag(tags.ERROR, True)
        context.span.set_tag("error.kind", error.__class__.__name__)
        context.span.log_kv(
            {
                "event": "error",
                "error.kind": error.__class__.__name__,
                "error.message": str(error),
                "error.stack": error.__traceback__,
                "error.object": error.__class__,
            }
        )
        context.close()

    def observe_query_after(
        self, conn: ConnectionProtocol, query: str, context: Scope, trx_context: Scope, **kwargs: Any
    ) -> None:
        """Finish query scope on query success."""
        context.close()
