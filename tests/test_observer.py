from typing import Any

import pytest

from asyncdb.aiomysql import QueryObserver, TransactionObserver, AioMySQLConnection, Transaction, TransactionFactory
from asyncdb.exceptions import ParseError
from .test_connection import config


@pytest.fixture(scope="function")
def connection():
    return AioMySQLConnection(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
    )


class TestQueryObserver(QueryObserver):
    def __init__(self) -> None:
        super().__init__()
        self.before: bool = False
        self.after: bool = False
        self.error: bool = False

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> None:
        self.before = True

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, **kwargs: Any) -> None:
        self.after = True

    def observe_query_error(
            self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, **kwargs: Any
    ) -> None:
        self.error = True

    def reset(self) -> None:
        self.before = False
        self.after = False
        self.error = False


class TestTransactionObserver(TransactionObserver):
    def __init__(self) -> None:
        super().__init__()
        self.start: bool = False
        self.commit: bool = False
        self.rollback: bool = False
        self.end: bool = False

        self.before: bool = False
        self.after: bool = False
        self.error: bool = False

    def observe_transaction_start(self, trx: Transaction, **kwargs: Any) -> Any:  # noqa: B027
        self.start = True

    def observe_transaction_end(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        self.end = True

    def observe_transaction_commit(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        self.commit = True

    def observe_transaction_rollback(self, trx: Transaction, context: Any, **kwargs: Any) -> None:  # noqa: B027
        self.rollback = True

    def observe_query_before(self, conn: AioMySQLConnection, query: str, trx_context: Any, **kwargs: Any) -> None:
        self.before = True

    def observe_query_after(self, conn: AioMySQLConnection, query: str, context: Any, trx_context: Any, **kwargs: Any) -> None:
        self.after = True

    def observe_query_error(
            self, conn: AioMySQLConnection, query: str, error: BaseException, context: Any, trx_context: Any, **kwargs: Any
    ) -> None:
        self.error = True

    def reset(self) -> None:
        self.start = False
        self.commit = False
        self.rollback = False
        self.end = False

        self.before = False
        self.after = False
        self.error = False


@pytest.mark.asyncio
async def test_query_observer(connection):
    observer = TestQueryObserver()
    connection.attach(observer)

    async with Transaction(connection) as trx:
        res = await trx.query("SELECT 1")
        await res.close()

        assert observer.before is True
        assert observer.after is True
        assert observer.error is False

        observer.reset()
        connection.detach(observer)

        res = await trx.query("SELECT 1")
        await res.close()

        assert observer.before is False
        assert observer.after is False
        assert observer.error is False

        connection.attach(observer)

        with pytest.raises(ParseError):
            await trx.query("PARSE ERROR")

        assert observer.before is True
        assert observer.after is False
        assert observer.error is True


@pytest.mark.asyncio
async def test_query_observer_from_factory():
    try:
        observer = TestQueryObserver()

        factory = TransactionFactory(
            config=config,
            connection_observers=[observer]
        )

        async with factory.transaction() as trx:
            res = await trx.query("SELECT 1")
            await res.close()

            assert observer.before is True
            assert observer.after is True
            assert observer.error is False

            observer.reset()

            with pytest.raises(ParseError):
                await trx.query("PARSE ERROR")

            assert observer.before is True
            assert observer.after is False
            assert observer.error is True
    finally:
        TransactionFactory._pool_cache.clear()


@pytest.mark.asyncio
async def test_transaction_observer(connection):
    observer = TestTransactionObserver()

    async with Transaction(connection, observers=[observer]) as trx:
        res = await trx.query("SELECT 1")
        await res.close()

        assert observer.start is True
        assert observer.end is False
        assert observer.commit is False
        assert observer.rollback is False
        assert observer.before is True
        assert observer.after is True
        assert observer.error is False

        observer.reset()

        with pytest.raises(ParseError):
            await trx.query("PARSE ERROR")

        assert observer.start is False
        assert observer.end is False
        assert observer.commit is False
        assert observer.rollback is False
        assert observer.before is True
        assert observer.after is False
        assert observer.error is True

    assert observer.end is True
    assert observer.commit is False
    assert observer.rollback is True

    observer.reset()

    async with Transaction(connection, observers=[observer]) as trx:
        res = await trx.query("SELECT 1")
        await res.close()

        await trx.commit()

    assert observer.start is True
    assert observer.end is True
    assert observer.commit is True
    assert observer.rollback is False
    assert observer.before is True
    assert observer.after is True
    assert observer.error is False


@pytest.mark.asyncio
async def test_transaction_observer_factory():
    observer = TestTransactionObserver()

    try:
        factory = TransactionFactory(
            config=config,
            transaction_observers=[observer]
        )

        async with factory.transaction() as trx:
            res = await trx.query("SELECT 1")
            await res.close()

            assert observer.start is True
            assert observer.end is False
            assert observer.commit is False
            assert observer.rollback is False
            assert observer.before is True
            assert observer.after is True
            assert observer.error is False

            observer.reset()

            with pytest.raises(ParseError):
                await trx.query("PARSE ERROR")

            assert observer.start is False
            assert observer.end is False
            assert observer.commit is False
            assert observer.rollback is False
            assert observer.before is True
            assert observer.after is False
            assert observer.error is True

        assert observer.end is True
        assert observer.commit is False
        assert observer.rollback is True

        observer.reset()

        async with factory.transaction() as trx:
            res = await trx.query("SELECT 1")
            await res.close()

            await trx.commit()

        assert observer.start is True
        assert observer.end is True
        assert observer.commit is True
        assert observer.rollback is False
        assert observer.before is True
        assert observer.after is True
        assert observer.error is False
    finally:
        TransactionFactory._pool_cache.clear()
