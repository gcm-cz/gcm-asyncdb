import asyncio
import os
from typing import Any

import pytest
import pytest_asyncio

from asyncdb.aiomysql import MySQLConfig, MySQLConnectionPool, Transaction
from asyncdb.exceptions import QueryError, LostConnection


@pytest_asyncio.fixture
async def pool():
    pool = MySQLConnectionPool(
        MySQLConfig(
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
            user=os.getenv("TEST_MYSQL_USER", "root"),
            password=os.getenv("TEST_MYSQL_PASSWORD", ""),
            database=os.getenv("TEST_MYSQL_DATABASE", "test"),
            max_spare_conns=5,
            min_spare_conns=3,
        ),
        keeper_task=False,
    )
    yield pool
    await pool.close()


def test_pool_init_errors():
    with pytest.raises(AttributeError, match="max_conn_lifetime must be greater or equal to zero"):
        MySQLConnectionPool(MySQLConfig(max_conn_lifetime=-1))

    with pytest.raises(AttributeError, match="max_conn_usage must be greater or equal to zero"):
        MySQLConnectionPool(MySQLConfig(max_conn_usage=-1))


@pytest.mark.asyncio
async def test_pool_counts_connections(pool):
    assert pool.current_pool_size == 0
    await pool._keeper_task_iteration()
    assert pool.current_pool_size == 3
    assert pool.current_pool_free == 3

    async with await pool.get_transaction() as trx:
        await trx.commit()
        assert pool.current_pool_size == 3
        assert pool.current_pool_free == 2

    assert pool.current_pool_size == 3
    assert pool.current_pool_free == 3


@pytest.mark.asyncio
async def test_pool_raises_connections(pool):
    await pool._keeper_task_iteration()

    async with await pool.get_transaction() as trx:
        await trx.commit()
        assert pool.current_pool_size == 3
        assert pool.current_pool_free == 2

        await pool._keeper_task_iteration()
        assert pool.current_pool_size == 4
        assert pool.current_pool_free == 3

    assert pool.current_pool_size == 4
    assert pool.current_pool_free == 4


@pytest.mark.asyncio
async def test_pool_dynamically_creates_connections(pool):
    assert pool.current_pool_size == 0
    assert pool.current_pool_free == 0

    async with await pool.get_transaction() as trx:
        assert pool.current_pool_size == 1
        assert pool.current_pool_free == 0
        await trx.rollback()

    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    await pool._keeper_task_iteration()

    assert pool.current_pool_size == 3
    assert pool.current_pool_free == 3


@pytest.mark.asyncio
async def test_pool_depletion():
    pool = MySQLConnectionPool(
        MySQLConfig(
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
            user=os.getenv("TEST_MYSQL_USER", "root"),
            password=os.getenv("TEST_MYSQL_PASSWORD", ""),
            database=os.getenv("TEST_MYSQL_DATABASE", "test"),
            max_pool_size=1,
        ),
        keeper_task=False,
    )
    assert pool.current_pool_size == 0
    assert pool.current_pool_free == 0

    await pool._keeper_task_iteration()

    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    async with await pool.get_transaction() as trx1:
        await pool._keeper_task_iteration()

        assert pool.current_pool_size == 1
        assert pool.current_pool_free == 0

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(pool.get_transaction(), timeout=0.1)

        await pool._keeper_task_iteration()

        assert pool.current_pool_size == 1
        assert pool.current_pool_free == 0

        await trx1.rollback()

    await pool._keeper_task_iteration()

    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    await pool._keeper_task_iteration()

    async with await pool.get_transaction() as trx1:
        await pool._keeper_task_iteration()

        assert pool.current_pool_size == 1
        assert pool.current_pool_free == 0

        await trx1.rollback()

    await pool._keeper_task_iteration()

    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    await pool.close()


@pytest.mark.asyncio
async def test_pool_discard_conns():
    pool = MySQLConnectionPool(
        MySQLConfig(
            max_pool_size=1,
            max_conn_usage=2,
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
            user=os.getenv("TEST_MYSQL_USER", "root"),
            password=os.getenv("TEST_MYSQL_PASSWORD", ""),
            database=os.getenv("TEST_MYSQL_DATABASE", "test"),
        ),
        keeper_task=False,
    )
    await pool._keeper_task_iteration()

    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    async with await pool.get_transaction() as trx1:
        assert trx1.connection.used == 1
        assert pool.connections_in_use == 1
        first_conn = trx1.connection
        await trx1.rollback()

    assert pool.connections_in_use == 0
    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1
    assert bool(pool) is True

    await pool._keeper_task_iteration()

    async with await pool.get_transaction() as trx2:
        assert pool.connections_in_use == 1
        assert trx2.connection.used == 2
        assert trx2.connection is first_conn
        await trx2.rollback()

    assert pool.connections_in_use == 0
    assert pool.current_pool_size == 0
    assert pool.current_pool_free == 0
    assert bool(pool) is False

    await pool._keeper_task_iteration()

    async with await pool.get_transaction() as trx3:
        assert pool.connections_in_use == 1
        assert trx3.connection.used == 1
        assert trx3.connection is not first_conn
        await trx3.rollback()

    await pool._keeper_task_iteration()

    assert pool.connections_in_use == 0
    assert pool.current_pool_size == 1
    assert pool.current_pool_free == 1

    await pool.close()


@pytest.mark.asyncio
async def test_remote_app():
    pool = MySQLConnectionPool(
        MySQLConfig(
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
            user=os.getenv("TEST_MYSQL_USER", "root"),
            password=os.getenv("TEST_MYSQL_PASSWORD", ""),
            database=os.getenv("TEST_MYSQL_DATABASE", "test"),
            max_pool_size=1,
            remote_app="test-remote-app",
        ),
        keeper_task=False,
    )
    await pool._keeper_task_iteration()

    # Pool has remote app
    assert pool.remote_app == "test-remote-app", "Pool remote app is missing."

    async with await pool.get_transaction() as trx:
        # Connection has remote app
        assert trx.connection.remote_app == "test-remote-app", "Connection remote app is missing."

        # Transaction has remote app
        assert trx.remote_app == "test-remote-app", "Transaction remote app is missing."

        try:
            await trx.query("SELECT INVALID")
        except QueryError as exc:
            # Exception has remote app
            assert exc.remote_app == "test-remote-app", "Exception remote app is missing."

    await pool.close()


@pytest.mark.asyncio
async def test_pool_connect_error(caplog):
    pool = MySQLConnectionPool(MySQLConfig(max_pool_size=1, host="invalid-host"), keeper_task=False)
    await pool._keeper_task_iteration()

    assert "Connection failed with" in caplog.text

    await pool.close()


class FailingTransaction(Transaction):
    async def _start_transaction(self) -> None:
        raise LostConnection("BEGIN", "Lost connection to MySQL server during query.", 0)


class FailingMySQLConnectionPool(MySQLConnectionPool):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.transaction_class = FailingTransaction


@pytest.mark.asyncio
async def test_pool_transaction_error():
    pool = FailingMySQLConnectionPool(
        MySQLConfig(
            host=os.getenv("TEST_MYSQL_HOST", "localhost"),
            port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
            user=os.getenv("TEST_MYSQL_USER", "root"),
            password=os.getenv("TEST_MYSQL_PASSWORD", ""),
            database=os.getenv("TEST_MYSQL_DATABASE", "test"),
            max_pool_size=10,
        ),
        keeper_task=False,
    )
    assert pool.connections_in_use == 0
    assert pool.current_pool_free == 0
    assert pool.current_pool_size == 0
    await pool._keeper_task_iteration()

    assert pool.connections_in_use == 0
    assert pool.current_pool_free == 5
    assert pool.current_pool_size == 5

    with pytest.raises(LostConnection):
        async with await pool.get_transaction():
            pass

    assert pool.connections_in_use == 0
    assert pool.current_pool_free == 4
    assert pool.current_pool_size == 4

    await pool._keeper_task_iteration()

    assert pool.connections_in_use == 0
    assert pool.current_pool_free == 5
    assert pool.current_pool_size == 5
