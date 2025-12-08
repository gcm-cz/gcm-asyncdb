import os
from typing import Any, AsyncGenerator

import pytest
from pymysql.constants import ER
from tenacity import stop_after_attempt

from asyncdb.aiomysql import AioMySQLConnection, MySQLConfig, QueryObserver, TransactionFactory
from asyncdb.exceptions import Deadlock

config = MySQLConfig(
    host=os.getenv("TEST_MYSQL_HOST", "localhost"),
    port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
    user=os.getenv("TEST_MYSQL_USER", "root"),
    password=os.getenv("TEST_MYSQL_PASSWORD", ""),
    database=os.getenv("TEST_MYSQL_DATABASE", "test"),
    max_pool_size=1,
)


class TestQueryObserver(QueryObserver):
    def __init__(self):
        super().__init__()
        self.QUERIES_EXECUTED = []

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:
        self.QUERIES_EXECUTED.append(query)

    def clear(self):
        self.QUERIES_EXECUTED.clear()


@pytest.fixture(scope="function")
def catch_queries() -> TestQueryObserver:
    return TestQueryObserver()


@pytest.fixture(scope="function")
async def factory(catch_queries: TestQueryObserver) -> AsyncGenerator[TransactionFactory, None]:
    factory = TransactionFactory(
        config=config,
        connection_observers=[catch_queries],
    )
    factory.ensure_pool()

    await factory.pool._keeper_task_iteration()

    yield factory

    for pool in TransactionFactory._pool_cache.values():
        await pool.close()

    TransactionFactory._pool_cache.clear()


@pytest.mark.asyncio
async def test_retry_reraises(factory: TransactionFactory, catch_queries: TestQueryObserver, caplog) -> None:
    catch_queries.clear()

    async with factory.transaction() as trx:
        caplog.clear()

        @trx.deadlock_retry(retry_stop=stop_after_attempt(2))
        async def retry_block() -> None:
            await trx.query("SELECT 'a'")
            raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

        # Ensure the deadlock is re-raised after retries
        with pytest.raises(Deadlock):
            await retry_block()
            await trx.commit()  # This line won't be called.

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-6:] == [
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK"
    ]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.
    for row in caplog.records[:-1]:
        assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_succeeds(factory: TransactionFactory, catch_queries: TestQueryObserver, caplog) -> None:
    catch_queries.clear()

    async with factory.transaction() as trx:
        caplog.clear()
        attempt = 0

        @trx.deadlock_retry
        async def retry_block() -> None:
            nonlocal attempt
            attempt += 1
            await trx.query("SELECT 'a'")
            if attempt < 3:
                raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

        # Ensure the block eventually succeeds.
        await retry_block()
        await trx.commit()

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-9:] == [
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "COMMIT"
    ]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.
    for row in caplog.records[:-1]:
        assert row.filename == "test_retry.py"
