from __future__ import annotations

import logging
from time import time
from typing import TYPE_CHECKING, Any

from tenacity import RetryCallState

from asyncdb._sl import _SL
from asyncdb.aiomysql.observer import QueryObserver, TransactionObserver

if TYPE_CHECKING:  # pragma: no cover
    from asyncdb.aiomysql.connection import AioMySQLConnection
    from asyncdb.aiomysql.transaction import Transaction


class LoggingObserver(QueryObserver):
    """
    Observer that logs all queries to the logger.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger to log queries to.
        """
        self.logger = logger

    def observe_query_before(
        self, conn: AioMySQLConnection, query: str, *, logger: logging.Logger | None = None, stacklevel: int = 0, **kwargs: Any
    ) -> float:
        """Log start of query execution."""
        (logger or self.logger).debug("Executing %s", query, stacklevel=_SL.get() + stacklevel)
        return time()

    def observe_query_after(
        self,
        conn: AioMySQLConnection,
        query: str,
        context: float,
        *,
        logger: logging.Logger | None = None,
        stacklevel: int = 0,
        **kwargs: Any,
    ) -> None:
        """Log end of query execution."""
        (logger or self.logger).debug("Took %1.4fs", time() - context, stacklevel=_SL.get() + stacklevel)

    def observe_query_error(
        self,
        conn: AioMySQLConnection,
        query: str,
        error: BaseException,
        context: float,
        *,
        logger: logging.Logger | None = None,
        stacklevel: int = 0,
        **kwargs: Any,
    ) -> None:
        """Log query error."""
        (logger or self.logger).debug("Took %1.4fs: %s", time() - context, str(error), stacklevel=_SL.get() + stacklevel)


class TransactionLoggingObserver(TransactionObserver):
    """
    Observer that logs transaction-related events.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger to log queries to.
        """
        self.logger = logger

    def observe_transaction_retry(self, trx: Transaction, context: Any, retry_state: RetryCallState) -> None:
        """Log retry attempt."""

        self.logger.warning(
            "Transaction retry #%d after %1.4fs due to %r",
            retry_state.attempt_number,
            retry_state.seconds_since_start,
            retry_state.outcome.exception(),
            stacklevel=_SL.get(),
        )
