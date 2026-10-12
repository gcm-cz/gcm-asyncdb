from collections.abc import AsyncGenerator

import pytest

from asyncdb.aiomysql import MySQLConfig, MySQLConnectionPool, SharedTransactionFactory, TransactionFactory


@pytest.fixture
async def clean_pool_cache() -> AsyncGenerator[None]:
    try:
        yield

    finally:
        for pool in TransactionFactory._pool_cache.values():
            await pool.close()

        TransactionFactory._pool_cache.clear()

        for pool in MySQLConnectionPool._pool_cache.values():
            await pool.close()

        MySQLConnectionPool._pool_cache.clear()


@pytest.mark.asyncio
async def test_pool_reports_closed(pool: MySQLConnectionPool) -> None:
    assert not pool.closed

    await pool.close()

    assert pool.closed


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_pool_cache")
@pytest.mark.parametrize("factory_class", [TransactionFactory, SharedTransactionFactory])
async def test_factory_replaces_closed_pool(
    factory_class: type[TransactionFactory] | type[SharedTransactionFactory], config: MySQLConfig
) -> None:
    """
    A pool closed by one factory must not be handed to a factory created later for the same config. The closed pool
    discards every connection on return, so each transaction would silently open a brand new connection.
    """
    first = factory_class(config)
    first.ensure_pool(keeper_task=False)
    closed_pool = first.pool
    await closed_pool.close()

    second = factory_class(config)
    second.ensure_pool(keeper_task=False)

    assert second.pool is not closed_pool
    assert not second.pool.closed

    async with second.transaction() as trx:
        assert await trx.get_scalar("SELECT 1") == 1
        await trx.commit()

    assert second.pool.current_pool_free == 1, "connection was discarded instead of returned to the pool"


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_pool_cache")
async def test_create_replaces_closed_pool(config: MySQLConfig) -> None:
    closed_pool = MySQLConnectionPool.create(config, keeper_task=False)
    await closed_pool.close()

    pool = MySQLConnectionPool.create(config, keeper_task=False)

    assert pool is not closed_pool
    assert not pool.closed
