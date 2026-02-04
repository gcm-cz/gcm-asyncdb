import logging
from typing import Any

import pytest

from asyncdb.aiomysql import AioMySQLConnection, MySQLConfig, Transaction, TransactionFactory
from asyncdb.asyncdb import QueryObserver, TransactionObserver
from asyncdb.exceptions import ImplicitRollback, ParseError


class QueryObserverForTest(QueryObserver):
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


class TransactionObserverForTest(TransactionObserver):
    def __init__(self) -> None:
        super().__init__()
        self.start: bool = False
        self.commit: bool = False
        self.rollback: bool = False
        self.end: bool = False

        self.before: bool = False
        self.after: bool = False
        self.error: bool = False

    def observe_transaction_start(self, trx: Transaction, **kwargs: Any) -> Any:
        self.start = True

    def observe_transaction_end(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
        self.end = True

    def observe_transaction_commit(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
        self.commit = True

    def observe_transaction_rollback(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
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
async def test_query_observer(connection: AioMySQLConnection) -> None:
    observer = QueryObserverForTest()
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

        await trx.commit()


@pytest.mark.asyncio
async def test_query_observer_from_factory(config: MySQLConfig) -> None:
    try:
        observer = QueryObserverForTest()

        factory = TransactionFactory(config=config, connection_observers=[observer])

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

            await trx.commit()
    finally:
        for pool in TransactionFactory._pool_cache.values():
            await pool.close()
        TransactionFactory._pool_cache.clear()


@pytest.mark.asyncio
async def test_transaction_observer(connection: AioMySQLConnection) -> None:
    observer = TransactionObserverForTest()

    with pytest.warns(ImplicitRollback):
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
async def test_transaction_observer_factory(config: MySQLConfig) -> None:
    observer = TransactionObserverForTest()

    try:
        factory = TransactionFactory(config=config, transaction_observers=[observer])

        with pytest.warns(ImplicitRollback):
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
        for pool in TransactionFactory._pool_cache.values():
            await pool.close()

        TransactionFactory._pool_cache.clear()


class QueryObserverArgsCapture(QueryObserver):
    """Observer that captures query pattern and args for testing."""

    def __init__(self) -> None:
        super().__init__()
        self.before_query: str | None = None
        self.before_query_pattern: str | None = None
        self.before_query_args: Any = None
        self.after_query: str | None = None
        self.after_query_pattern: str | None = None
        self.after_query_args: Any = None
        self.error_query: str | None = None
        self.error_query_pattern: str | None = None
        self.error_query_args: Any = None

    def observe_query_before(
        self, conn: AioMySQLConnection, query: str, *, query_pattern: str, query_args: Any, **kwargs: Any
    ) -> None:
        self.before_query = query
        self.before_query_pattern = query_pattern
        self.before_query_args = query_args

    def observe_query_after(
        self, conn: AioMySQLConnection, query: str, context: Any, *, query_pattern: str, query_args: Any, **kwargs: Any
    ) -> None:
        self.after_query = query
        self.after_query_pattern = query_pattern
        self.after_query_args = query_args

    def observe_query_error(
        self,
        conn: AioMySQLConnection,
        query: str,
        error: BaseException,
        context: Any,
        *,
        query_pattern: str,
        query_args: Any,
        **kwargs: Any,
    ) -> None:
        self.error_query = query
        self.error_query_pattern = query_pattern
        self.error_query_args = query_args

    def reset(self) -> None:
        self.before_query = None
        self.before_query_pattern = None
        self.before_query_args = None
        self.after_query = None
        self.after_query_pattern = None
        self.after_query_args = None
        self.error_query = None
        self.error_query_pattern = None
        self.error_query_args = None


class TransactionObserverArgsCapture(TransactionObserver):
    """Observer that captures query pattern and args for testing."""

    def __init__(self) -> None:
        super().__init__()
        self.before_query: str | None = None
        self.before_query_pattern: str | None = None
        self.before_query_args: Any = None
        self.after_query: str | None = None
        self.after_query_pattern: str | None = None
        self.after_query_args: Any = None
        self.error_query: str | None = None
        self.error_query_pattern: str | None = None
        self.error_query_args: Any = None

    def observe_transaction_start(self, trx: Transaction, **kwargs: Any) -> Any:
        pass

    def observe_transaction_end(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
        pass

    def observe_transaction_commit(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
        pass

    def observe_transaction_rollback(self, trx: Transaction, context: Any, **kwargs: Any) -> None:
        pass

    def observe_query_before(
        self, conn: AioMySQLConnection, query: str, trx_context: Any, *, query_pattern: str, query_args: Any, **kwargs: Any
    ) -> None:
        self.before_query = query
        self.before_query_pattern = query_pattern
        self.before_query_args = query_args

    def observe_query_after(
        self,
        conn: AioMySQLConnection,
        query: str,
        context: Any,
        trx_context: Any,
        *,
        query_pattern: str,
        query_args: Any,
        **kwargs: Any,
    ) -> None:
        self.after_query = query
        self.after_query_pattern = query_pattern
        self.after_query_args = query_args

    def observe_query_error(
        self,
        conn: AioMySQLConnection,
        query: str,
        error: BaseException,
        context: Any,
        trx_context: Any,
        *,
        query_pattern: str,
        query_args: Any,
        **kwargs: Any,
    ) -> None:
        self.error_query = query
        self.error_query_pattern = query_pattern
        self.error_query_args = query_args

    def reset(self) -> None:
        self.before_query = None
        self.before_query_pattern = None
        self.before_query_args = None
        self.after_query = None
        self.after_query_pattern = None
        self.after_query_args = None
        self.error_query = None
        self.error_query_pattern = None
        self.error_query_args = None


@pytest.mark.asyncio
async def test_query_observer_receives_query_pattern_and_args(connection: AioMySQLConnection) -> None:
    """Test that QueryObserver receives correct query pattern and args in all callbacks."""
    observer = QueryObserverArgsCapture()
    connection.attach(observer)

    query_pattern = "SELECT %s, %s"
    query_args = (1, "test")

    async with Transaction(connection) as trx:
        res = await trx.query(query_pattern, *query_args)
        await res.close()

        # Verify before callback
        assert observer.before_query_pattern == query_pattern
        assert observer.before_query_args == query_args
        assert observer.before_query == "SELECT 1, 'test'"  # Should be formatted query

        # Verify after callback
        assert observer.after_query_pattern == query_pattern
        assert observer.after_query_args == query_args
        assert observer.after_query == "SELECT 1, 'test'"  # Should be formatted query

        observer.reset()

        # Test with query that causes error
        error_pattern = "PARSE %s ERROR"
        error_args = ("syntax",)

        with pytest.raises(ParseError):
            await trx.query(error_pattern, *error_args)

        # Verify error callback
        assert observer.before_query_pattern == error_pattern
        assert observer.before_query_args == error_args
        assert observer.before_query == "PARSE 'syntax' ERROR"  # Should be formatted query
        assert observer.error_query_pattern == error_pattern
        assert observer.error_query_args == error_args
        assert observer.error_query == "PARSE 'syntax' ERROR"  # Should be formatted query

        await trx.commit()

    connection.detach(observer)


@pytest.mark.asyncio
async def test_query_observer_with_no_args(connection: AioMySQLConnection) -> None:
    """Test that QueryObserver receives empty args when no parameters are provided."""
    observer = QueryObserverArgsCapture()
    connection.attach(observer)

    query_pattern = "SELECT 1"

    async with Transaction(connection) as trx:
        res = await trx.query(query_pattern)
        await res.close()

        # Verify callbacks receive the same query as pattern when no args
        assert observer.before_query_pattern == query_pattern
        assert observer.before_query_args == ()
        assert observer.after_query_pattern == query_pattern
        assert observer.after_query_args == ()

        # Query should match pattern when no substitution is needed
        assert observer.before_query == query_pattern
        assert observer.after_query == query_pattern

        await trx.commit()

    connection.detach(observer)


@pytest.mark.asyncio
async def test_transaction_observer_receives_query_pattern_and_args(connection: AioMySQLConnection) -> None:
    """Test that TransactionObserver receives correct query pattern and args in all callbacks."""
    observer = TransactionObserverArgsCapture()

    query_pattern = "SELECT %s, %s"
    query_args = (42, "data")

    async with Transaction(connection, observers=[observer]) as trx:
        res = await trx.query(query_pattern, *query_args)
        await res.close()

        # Verify before callback
        assert observer.before_query_pattern == query_pattern
        assert observer.before_query_args == query_args
        assert observer.before_query == "SELECT 42, 'data'"  # Should be formatted query

        # Verify after callback
        assert observer.after_query_pattern == query_pattern
        assert observer.after_query_args == query_args
        assert observer.after_query == "SELECT 42, 'data'"  # Should be formatted query

        observer.reset()

        # Test with query that causes error
        error_pattern = "PARSE %s ERROR"
        error_args = ("invalid",)

        with pytest.raises(ParseError):
            await trx.query(error_pattern, *error_args)

        # Verify error callback
        assert observer.before_query_pattern == error_pattern
        assert observer.before_query_args == error_args
        assert observer.before_query == "PARSE 'invalid' ERROR"  # Should be formatted query
        assert observer.error_query_pattern == error_pattern
        assert observer.error_query_args == error_args
        assert observer.error_query == "PARSE 'invalid' ERROR"  # Should be formatted query

        await trx.commit()


@pytest.mark.asyncio
async def test_transaction_observer_with_no_args(connection: AioMySQLConnection) -> None:
    """Test that TransactionObserver receives empty args when no parameters are provided."""
    observer = TransactionObserverArgsCapture()

    query_pattern = "SELECT 2"

    async with Transaction(connection, observers=[observer]) as trx:
        res = await trx.query(query_pattern)
        await res.close()

        # Verify callbacks receive the same query as pattern when no args
        assert observer.before_query_pattern == query_pattern
        assert observer.before_query_args == ()
        assert observer.after_query_pattern == query_pattern
        assert observer.after_query_args == ()

        # Query should match pattern when no substitution is needed
        assert observer.before_query == query_pattern
        assert observer.after_query == query_pattern

        await trx.commit()


@pytest.mark.asyncio
async def test_query_observer_with_multiple_queries(connection: AioMySQLConnection) -> None:
    """Test that QueryObserver correctly tracks different queries with different patterns and args."""
    observer = QueryObserverArgsCapture()
    connection.attach(observer)

    async with Transaction(connection) as trx:
        # First query
        pattern1 = "SELECT %s"
        args1 = (100,)
        res = await trx.query(pattern1, *args1)
        await res.close()

        assert observer.before_query_pattern == pattern1
        assert observer.before_query_args == args1
        assert observer.after_query_pattern == pattern1
        assert observer.after_query_args == args1

        observer.reset()

        # Second query with different pattern and args
        pattern2 = "SELECT %s, %s, %s"
        args2 = ("a", "b", "c")
        res = await trx.query(pattern2, *args2)
        await res.close()

        assert observer.before_query_pattern == pattern2
        assert observer.before_query_args == args2
        assert observer.after_query_pattern == pattern2
        assert observer.after_query_args == args2

        await trx.commit()

    connection.detach(observer)


@pytest.mark.asyncio
async def test_transaction_observer_with_multiple_queries(connection: AioMySQLConnection) -> None:
    """Test that TransactionObserver correctly tracks different queries with different patterns and args."""
    observer = TransactionObserverArgsCapture()

    async with Transaction(connection, observers=[observer]) as trx:
        # First query
        pattern1 = "SELECT %s"
        args1 = (200,)
        res = await trx.query(pattern1, *args1)
        await res.close()

        assert observer.before_query_pattern == pattern1
        assert observer.before_query_args == args1
        assert observer.after_query_pattern == pattern1
        assert observer.after_query_args == args1

        observer.reset()

        # Second query with different pattern and args
        pattern2 = "SELECT %s, %s, %s"
        args2 = ("x", "y", "z")
        res = await trx.query(pattern2, *args2)
        await res.close()

        assert observer.before_query_pattern == pattern2
        assert observer.before_query_args == args2
        assert observer.after_query_pattern == pattern2
        assert observer.after_query_args == args2

        await trx.commit()


@pytest.mark.asyncio
async def test_observer_receives_query_pattern_and_query_args_correctly(connection: AioMySQLConnection) -> None:
    """Test that both QueryObserver and TransactionObserver receive query pattern and args correctly."""
    query_observer = QueryObserverArgsCapture()
    trx_observer = TransactionObserverArgsCapture()

    connection.attach(query_observer)

    query_pattern = "SELECT %s, %s, %s"
    query_args = (1, 2, 3)

    async with Transaction(connection, observers=[trx_observer]) as trx:
        res = await trx.query(query_pattern, *query_args)
        await res.close()

        # Both observers should capture the same pattern and args
        assert query_observer.before_query_pattern == query_pattern
        assert query_observer.before_query_args == query_args
        assert query_observer.after_query_pattern == query_pattern
        assert query_observer.after_query_args == query_args

        assert trx_observer.before_query_pattern == query_pattern
        assert trx_observer.before_query_args == query_args
        assert trx_observer.after_query_pattern == query_pattern
        assert trx_observer.after_query_args == query_args

        # Both should have the same formatted query
        assert query_observer.before_query == trx_observer.before_query
        assert query_observer.after_query == trx_observer.after_query

        await trx.commit()

    connection.detach(query_observer)
