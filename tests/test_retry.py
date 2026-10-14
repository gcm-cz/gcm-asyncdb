from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from pymysql.constants import ER
from tenacity import stop_after_attempt

from asyncdb.aiomysql import SharedTransactionFactory, Transaction, TransactionFactory, TransactionIsolationLevel
from asyncdb.exceptions import Deadlock, ImplicitRollback

from .conftest import CatchQueryObserver


@pytest.mark.asyncio
async def test_retry_reraises(
    factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture
) -> None:
    catch_queries.clear()
    caplog.clear()

    with pytest.warns(ImplicitRollback):
        async with factory.transaction() as trx:

            @trx.retry(retry_stop=stop_after_attempt(2))
            async def retry_block() -> None:
                await trx.query("SELECT 'a'")
                raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

            # Ensure the deadlock is re-raised after retries
            with pytest.raises(Deadlock):
                await retry_block()
                await trx.commit()  # This line won't be called.

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-6:] == ["BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK"]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.

    ATTEMPTS = 2
    QUERIES = 3  # BEGIN, SELECT, ROLLBACK
    LOG_PER_QUERY = 2  # "Executing ...", "Took"
    assert len([row for row in caplog.records if row.name == "db.mysql.test"]) == (
        (ATTEMPTS * QUERIES * LOG_PER_QUERY) + (ATTEMPTS - 1)  # Last attempt fails with original exception, no retry is logged.
    )

    for row in caplog.records:
        if row.name == "db.mysql.test":
            assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_succeeds(
    factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture
) -> None:
    catch_queries.clear()
    caplog.clear()

    async with factory.transaction() as trx:
        attempt = 0

        @trx.retry
        async def retry_block() -> None:
            nonlocal attempt
            attempt += 1
            await trx.query("SELECT 'a'")
            if attempt < 3:
                raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

        # Ensure the block eventually succeeds.
        with pytest.warns(ImplicitRollback):
            await retry_block()

        await trx.commit()

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-9:] == [
        "BEGIN",
        "SELECT 'a'",
        "ROLLBACK",
        "BEGIN",
        "SELECT 'a'",
        "ROLLBACK",
        "BEGIN",
        "SELECT 'a'",
        "COMMIT",
    ]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.

    ATTEMPTS = 3
    QUERIES = 3  # BEGIN, SELECT, ROLLBACK/COMMIT
    LOG_PER_QUERY = 2  # "Executing ...", "Took"
    assert len([row for row in caplog.records if row.name == "db.mysql.test"]) == (
        (ATTEMPTS * QUERIES * LOG_PER_QUERY) + (ATTEMPTS - 1)  # Last attempt fails with original exception, no retry is logged.
    )

    for row in caplog.records[:-1]:
        if row.name == "db.mysql.test":
            assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_context_reraises(
    factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture
) -> None:
    catch_queries.clear()
    caplog.clear()

    with pytest.warns(ImplicitRollback):
        with pytest.raises(Deadlock):
            async for attempt in factory.retry(retry_stop=stop_after_attempt(3)):
                async with attempt as trx:
                    await trx.query("SELECT 'a'")
                    raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-6:] == ["BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK"]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.
    ATTEMPTS = 3
    QUERIES = 3  # BEGIN, SELECT, ROLLBACK/COMMIT
    LOG_PER_QUERY = 2  # "Executing ...", "Took"
    assert len([row for row in caplog.records if row.name == "db.mysql.test"]) == (
        (ATTEMPTS * QUERIES * LOG_PER_QUERY) + (ATTEMPTS - 1)  # Last attempt fails with original exception, no retry is logged.
    )

    for row in caplog.records:
        if row.name == "db.mysql.test":
            assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_context_succeeds(
    factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture
) -> None:
    catch_queries.clear()
    caplog.clear()

    attempt_no = 0
    with pytest.warns(ImplicitRollback):
        async for attempt in factory.retry():
            async with attempt as trx:
                attempt_no += 1
                await trx.query("SELECT 'a'")
                if attempt_no < 3:
                    raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

                # Ensure the block eventually succeeds.
                await trx.commit()

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-9:] == [
        "BEGIN",
        "SELECT 'a'",
        "ROLLBACK",
        "BEGIN",
        "SELECT 'a'",
        "ROLLBACK",
        "BEGIN",
        "SELECT 'a'",
        "COMMIT",
    ]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.
    ATTEMPTS = 3
    QUERIES = 3  # BEGIN, SELECT, ROLLBACK/COMMIT
    LOG_PER_QUERY = 2  # "Executing ...", "Took"
    assert len([row for row in caplog.records if row.name == "db.mysql.test"]) == (
        (ATTEMPTS * QUERIES * LOG_PER_QUERY) + (ATTEMPTS - 1)  # Last attempt fails with original exception, no retry is logged.
    )

    for row in caplog.records:
        if row.name == "db.mysql.test":
            assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_other_exception_not_retried(factory: TransactionFactory, catch_queries: CatchQueryObserver) -> None:
    catch_queries.clear()

    with pytest.raises(ValueError):
        with pytest.warns(ImplicitRollback):
            async for attempt in factory.retry():
                async with attempt as trx:
                    await trx.query("SELECT 1")
                    raise ValueError("test")

    assert catch_queries.QUERIES_EXECUTED == ["BEGIN", "SELECT 1", "ROLLBACK"]


@pytest.mark.asyncio
async def test_retry_other_exception_not_retried_shared_factory(
    shared_factory: SharedTransactionFactory, catch_queries: CatchQueryObserver
) -> None:
    catch_queries.clear()

    with pytest.raises(ValueError):
        with pytest.warns(ImplicitRollback):
            async for attempt in shared_factory.retry():
                async with attempt as trx:
                    await trx.query("SELECT 1")
                    raise ValueError("test")

    assert catch_queries.QUERIES_EXECUTED == ["BEGIN", "SELECT 1", "ROLLBACK"]


@asynccontextmanager
async def _isolation_probe(
    db: TransactionFactory | SharedTransactionFactory,
) -> AsyncIterator[Callable[[Transaction], Awaitable[bool]]]:
    """
    Yields a probe telling whether `trx` sees a row committed by another connection after `trx` already read the table. That
    holds under READ COMMITTED, but not under the server's default REPEATABLE READ, whose snapshot hides it. (`@@tx_isolation`
    cannot tell, as `SET TRANSACTION ISOLATION LEVEL` leaves the session value untouched.)
    """
    async with db.transaction() as trx:
        await trx.query("CREATE TABLE IF NOT EXISTS `retry_isolation` (`id` INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB")
        await trx.commit()

    async def sees_concurrent_commit(trx: Transaction) -> bool:
        before = await trx.get_scalar("SELECT COUNT(*) FROM `retry_isolation`")

        async with db.transaction() as other:
            await other.query("INSERT INTO `retry_isolation` () VALUES ()")
            await other.commit()

        return await trx.get_scalar("SELECT COUNT(*) FROM `retry_isolation`") != before

    try:
        yield sees_concurrent_commit

    finally:
        async with db.transaction() as trx:
            await trx.query("DROP TABLE `retry_isolation`")
            await trx.commit()


async def _retry_context_isolation(db: TransactionFactory | SharedTransactionFactory) -> list[bool]:
    visible = []
    async with _isolation_probe(db) as sees_concurrent_commit:
        with pytest.warns(ImplicitRollback):
            async for attempt in db.retry(isolation_level=TransactionIsolationLevel.READ_COMMITTED):
                async with attempt as trx:
                    visible.append(await sees_concurrent_commit(trx))
                    if len(visible) < 3:
                        raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

                    await trx.commit()

    return visible


@pytest.mark.asyncio
async def test_retry_context_applies_isolation_level(factory: TransactionFactory) -> None:
    # Every attempt, including the ones after a rollback, runs at the requested isolation level.
    assert await _retry_context_isolation(factory) == [True, True, True]


@pytest.mark.asyncio
async def test_retry_context_applies_isolation_level_shared_factory(shared_factory: SharedTransactionFactory) -> None:
    # Every attempt, including the ones after a rollback, runs at the requested isolation level.
    assert await _retry_context_isolation(shared_factory) == [True, True, True]


@pytest.mark.asyncio
async def test_retry_decorator_reapplies_isolation_level(shared_factory: SharedTransactionFactory) -> None:
    visible = []
    async with _isolation_probe(shared_factory) as sees_concurrent_commit:
        async with shared_factory.transaction(isolation_level=TransactionIsolationLevel.READ_COMMITTED) as trx:

            @trx.retry
            async def retry_block() -> None:
                visible.append(await sees_concurrent_commit(trx))
                if len(visible) < 3:
                    raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

            await retry_block()
            await trx.commit()

    # Each reset of the transaction between attempts starts it again at the requested isolation level.
    assert visible == [True, True, True]
