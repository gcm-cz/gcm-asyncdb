from __future__ import annotations

import contextlib
import logging
from contextvars import ContextVar
from time import time
from typing import Any, ClassVar, Generic

from tenacity import RetryCallState

from asyncdb._sl import _SL
from asyncdb.asyncdb.observer import GenericQueryObserver, GenericTransactionObserver
from asyncdb.generics import ConnectionTypeT, TransactionTypeT


class LoggingObserver(Generic[ConnectionTypeT], GenericQueryObserver[float, ConnectionTypeT]):
    """
    Observer that logs all queries to the logger.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger to log queries to.
        """
        self.logger = logger

    def observe_query_before(
        self, conn: ConnectionTypeT, query: str, *, logger: logging.Logger | None = None, stacklevel: int = 0, **kwargs: Any
    ) -> float:
        """Log start of query execution."""
        trx_id = TransactionLoggingObserver.trx_id.get()
        if trx_id is not None:
            (logger or self.logger).debug("Executing %s [trx %d]", query, trx_id, stacklevel=_SL.get() + stacklevel)
        else:
            (logger or self.logger).debug("Executing %s", query, stacklevel=_SL.get() + stacklevel)
        return time()

    def observe_query_after(
        self,
        conn: ConnectionTypeT,
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
        conn: ConnectionTypeT,
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


class TransactionLoggingObserver(
    Generic[ConnectionTypeT, TransactionTypeT], GenericTransactionObserver[int | None, None, ConnectionTypeT, TransactionTypeT]
):
    """
    Observer that logs transaction-related events.
    """

    trx_id: ClassVar[ContextVar[int | None]] = ContextVar("asyncdb.aiomysql.trx_id", default=None)

    def __init__(self, logger: logging.Logger) -> None:
        """
        :param logger: Logger to log queries to.
        """
        self.logger = logger

    def observe_transaction_before(self, trx: TransactionTypeT, **kwargs: Any) -> Any:
        """Store transaction ID before starting."""
        return self.__class__.trx_id.set(trx.trx_id)

    def observe_transaction_retry(self, trx: TransactionTypeT, context: Any, retry_state: RetryCallState) -> None:
        """Log retry attempt."""

        self.logger.warning(
            "Transaction retry #%d after %1.4fs due to %r",
            retry_state.attempt_number,
            retry_state.seconds_since_start,
            retry_state.outcome.exception(),
            stacklevel=_SL.get(),
        )

    def observe_transaction_end(self, trx: TransactionTypeT, context: Any, **kwargs: Any) -> None:
        """Restore transaction ID at the end."""
        with contextlib.suppress(ValueError):
            self.__class__.trx_id.reset(context)
