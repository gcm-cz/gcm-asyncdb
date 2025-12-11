import warnings
from contextlib import contextmanager

import pytest
from pymysql.constants import ER
from tenacity import stop_after_attempt

from asyncdb.aiomysql import TransactionFactory
from asyncdb.exceptions import Deadlock, ImplicitRollback

from .conftest import CatchQueryObserver


@pytest.mark.asyncio
async def test_retry_reraises(factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture) -> None:
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
    assert catch_queries.QUERIES_EXECUTED[-6:] == [
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK"
    ]

    # Ensure the deadlock is logged.
    assert "Simulated deadlock" in caplog.text

    # Ensure all log records are logged from this test file, and not from the library code.
    # The last log is returning connection to pool, which is logged from pool, so we skip it.

    ATTEMPTS = 2
    QUERIES = 3  # BEGIN, SELECT, ROLLBACK
    LOG_PER_QUERY = 2  # "Executing ...", "Took"
    assert len([row for row in caplog.records if row.name == "db.mysql.test"]) == (
        (ATTEMPTS * QUERIES * LOG_PER_QUERY) +
        (ATTEMPTS - 1)  # Last attempt fails with original exception, no retry is logged.
    )

    for row in caplog.records:
        if row.name == "db.mysql.test":
            assert row.filename == "test_retry.py"


@pytest.mark.asyncio
async def test_retry_succeeds(factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture) -> None:
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
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "COMMIT"
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
async def test_retry_context_reraises(factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture) -> None:
    catch_queries.clear()
    caplog.clear()

    with pytest.warns(ImplicitRollback):
        with pytest.raises(Deadlock):
            async for attempt in factory.retry(retry_stop=stop_after_attempt(3)):
                async with attempt as trx:
                    await trx.query("SELECT 'a'")
                    raise Deadlock("SELECT 'a'", "Simulated deadlock", ER.LOCK_DEADLOCK, trx.remote_app)

    # Ensure all the transaction queries were executed.
    assert catch_queries.QUERIES_EXECUTED[-6:] == [
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK"
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
async def test_retry_context_succeeds(factory: TransactionFactory, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture) -> None:
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
        "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "ROLLBACK", "BEGIN", "SELECT 'a'", "COMMIT"
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
