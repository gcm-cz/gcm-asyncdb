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
@pytest.mark.parametrize("factory_class", [TransactionFactory, SharedTransactionFactory])
async def test_factory_reused_after_its_pool_was_closed(
    factory_class: type[TransactionFactory] | type[SharedTransactionFactory], config: MySQLConfig
) -> None:
    """
    A long-lived factory (e.g. module-level singleton) used again after its pool was closed, e.g. by a second application
    instance in the same process, gets an open pool on the next transaction.
    """
    factory = factory_class(config)
    factory.ensure_pool(keeper_task=False)
    closed_pool = factory.pool
    await closed_pool.close()

    async with factory.transaction() as trx:
        assert await trx.get_scalar("SELECT 1") == 1
        await trx.commit()

    assert factory.pool is not closed_pool
    assert not factory.pool.closed
    assert factory.pool.current_pool_free > 0, "connection was discarded instead of returned to the pool"


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_pool_cache")
@pytest.mark.parametrize("factory_class", [TransactionFactory, SharedTransactionFactory])
async def test_closing_through_factory_never_closes_replacement_pool(
    factory_class: type[TransactionFactory] | type[SharedTransactionFactory], config: MySQLConfig
) -> None:
    """
    `factory.pool.close()` closes the pool that factory used. When a shared pool was closed and another factory has
    replaced it meanwhile, closing through a factory still bound to the old pool must not close the replacement.
    """
    first = factory_class(config)
    second = factory_class(config)
    first.ensure_pool(keeper_task=False)
    second.ensure_pool(keeper_task=False)
    shared_pool = first.pool
    assert second.pool is shared_pool

    await second.pool.close()

    third = factory_class(config)
    third.ensure_pool(keeper_task=False)
    replacement = third.pool
    assert replacement is not shared_pool

    await first.pool.close()

    assert first.pool is shared_pool
    assert not replacement.closed

    async with third.transaction() as trx:
        await trx.commit()

    assert third.pool is replacement
    assert replacement.current_pool_free == 1, "connection was discarded instead of returned to the pool"


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_pool_cache")
@pytest.mark.parametrize("factory_class", [TransactionFactory, SharedTransactionFactory])
async def test_pool_property_does_not_create_pool_to_close(
    factory_class: type[TransactionFactory] | type[SharedTransactionFactory], config: MySQLConfig
) -> None:
    """
    Two factories sharing a pool both close it on shutdown. The second `factory.pool.close()` must get the already closed
    pool, not a new pool created only to be closed.
    """
    first = factory_class(config)
    first.ensure_pool(keeper_task=False)
    closed_pool = first.pool
    await closed_pool.close()

    second = factory_class(config)

    assert second.pool is closed_pool
    assert TransactionFactory._pool_cache[first._cache_key] is closed_pool


@pytest.mark.asyncio
@pytest.mark.usefixtures("clean_pool_cache")
async def test_create_replaces_closed_pool(config: MySQLConfig) -> None:
    closed_pool = MySQLConnectionPool.create(config, keeper_task=False)
    await closed_pool.close()

    pool = MySQLConnectionPool.create(config, keeper_task=False)

    assert pool is not closed_pool
    assert not pool.closed
