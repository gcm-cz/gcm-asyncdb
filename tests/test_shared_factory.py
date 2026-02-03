import logging
from collections.abc import AsyncGenerator

import pytest

from asyncdb.aiomysql import MySQLConfig, SharedRollbackMode, SharedTransactionFactory
from asyncdb.exceptions import ImplicitRollback, SharedRollback


@pytest.fixture
async def factory(config: MySQLConfig) -> AsyncGenerator[SharedTransactionFactory]:
    factory = SharedTransactionFactory(
        config=config,
    )
    factory.ensure_pool(keeper_task=False)

    await factory.pool._keeper_task_iteration()

    try:
        yield factory

    finally:
        for pool in SharedTransactionFactory._pool_cache.values():
            await pool.close()

        SharedTransactionFactory._pool_cache.clear()


async def somewhere_deep_in_code(
    expected_connection_id: int, expected_transaction_id: int, factory: SharedTransactionFactory
) -> None:
    """
    Opens a shared transaction and verifies that it uses the expected connection (therefore sharing the same transaction).
    """

    async with factory.transaction(shared=True) as transaction:
        logging.warning("%r", transaction)
        assert id(transaction.connection) == expected_connection_id
        assert transaction.trx_id == expected_transaction_id
        assert await transaction.get_scalar("SELECT 1") == 1
        await transaction.commit()

    assert not await transaction.is_committed()


async def non_shared_transaction_should_not_overwrite_context(
    expected_connection_id: int, expected_transaction_id: int, factory: SharedTransactionFactory
) -> None:
    """
    Tries to open a non-shared transaction inside a shared transaction context, which should not affect the shared context.
    """

    async with factory.transaction() as transaction:
        assert id(transaction.connection) != expected_connection_id
        assert transaction.trx_id != expected_transaction_id
        assert await transaction.get_scalar("SELECT 3") == 3
        await transaction.commit()

    assert await transaction.is_committed()


@pytest.mark.asyncio
async def test_transaction_propagated(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    """
    Tests whether a shared transaction context is properly propagated through nested calls.
    """
    async with factory.transaction() as top_level_transaction:
        expected_connection_id = id(top_level_transaction.connection)
        expected_transaction_id = top_level_transaction.trx_id

        await non_shared_transaction_should_not_overwrite_context(expected_connection_id, expected_transaction_id, factory)

        await somewhere_deep_in_code(expected_connection_id, expected_transaction_id, factory)

        # Ensure the transaction is still useable after the nested call
        assert await top_level_transaction.get_scalar("SELECT 2") == 2
        await top_level_transaction.commit()

    assert await top_level_transaction.is_committed()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_no_shared_transaction(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    """
    Tests that new transaction is created when no shared transaction context exists.
    """
    async with factory.transaction(shared=True) as transaction1:
        connection_id1 = id(transaction1.connection)
        trx_id1 = transaction1.trx_id
        assert await transaction1.get_scalar("SELECT 1") == 1
        await transaction1.commit()

    assert await transaction1.is_committed()

    async with factory.transaction(shared=True) as transaction2:
        connection_id2 = id(transaction2.connection)
        assert trx_id1 != transaction2.trx_id
        assert connection_id1 != connection_id2
        assert await transaction2.get_scalar("SELECT 2") == 2
        await transaction2.rollback()

    assert await transaction2.is_committed()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_non_shared_rollback_warning(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    """
    Tests that a warning is logged when a shared transaction is rolled back.
    """
    with pytest.warns(ImplicitRollback):
        async with factory.transaction() as transaction:
            assert await transaction.get_scalar("SELECT 1") == 1

    assert await transaction.is_committed()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_shared_rollback_warning(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    """
    Tests that a warning is logged when a shared transaction is rolled back.
    """
    async with factory.transaction() as transaction:
        with pytest.warns(ImplicitRollback):
            async with factory.transaction(shared=True) as shared_transaction:
                assert await shared_transaction.get_scalar("SELECT 1") == 1

        await transaction.commit()

    assert await transaction.is_committed()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_callable_shared_factory(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    async for transaction in factory():
        expected_connection_id = id(transaction.connection)
        expected_transaction_id = transaction.trx_id
        async for shared_transaction in factory():
            assert id(shared_transaction.connection) == expected_connection_id
            assert shared_transaction.trx_id == expected_transaction_id
            assert await shared_transaction.get_scalar("SELECT 1") == 1
            await shared_transaction.rollback()

        assert await shared_transaction.is_committed() is False
        assert await transaction.is_committed() is False

        assert await transaction.get_scalar("SELECT 2") == 2
        await transaction.commit()

    assert await shared_transaction.is_committed() is True

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_callable_shared_factory_warns_rollback(
    factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture
) -> None:
    async for transaction in factory():
        expected_connection_id = id(transaction.connection)
        expected_transaction_id = transaction.trx_id

        with pytest.warns(ImplicitRollback):
            async for shared_transaction in factory():
                assert id(shared_transaction.connection) == expected_connection_id
                assert shared_transaction.trx_id == expected_transaction_id
                assert await shared_transaction.get_scalar("SELECT 1") == 1

        assert await shared_transaction.is_committed() is False
        assert await transaction.is_committed() is False

        assert await transaction.get_scalar("SELECT 2") == 2
        await transaction.commit()

    assert await shared_transaction.is_committed() is True

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_propagate_rollback_ignore(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    async with factory.transaction(shared_rollback_mode=SharedRollbackMode.IGNORE) as top_level_transaction:
        async with factory.transaction(shared=True) as shared_transaction:
            await shared_transaction.rollback()

        await top_level_transaction.commit()

    assert "ROLLBACK" not in caplog.text
    assert "COMMIT" in caplog.text

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_propagate_rollback_propagate(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    async with factory.transaction(shared_rollback_mode=SharedRollbackMode.PROPAGATE) as top_level_transaction:
        async with factory.transaction(shared=True) as shared_transaction:
            await shared_transaction.rollback()

        await top_level_transaction.commit()

    assert "ROLLBACK" in caplog.text
    assert "COMMIT" not in caplog.text

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_propagate_rollback_warn(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    async with factory.transaction(shared_rollback_mode=SharedRollbackMode.WARN) as top_level_transaction:
        async with factory.transaction(shared=True) as shared_transaction:
            await shared_transaction.rollback()

        with pytest.warns(SharedRollback):
            await top_level_transaction.commit()

    assert "ROLLBACK" in caplog.text
    assert "COMMIT" not in caplog.text

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"


@pytest.mark.asyncio
async def test_propagate_rollback_raise(factory: SharedTransactionFactory, caplog: pytest.LogCaptureFixture) -> None:
    async with factory.transaction(shared_rollback_mode=SharedRollbackMode.RAISE) as top_level_transaction:
        async with factory.transaction(shared=True) as shared_transaction:
            await shared_transaction.rollback()

        with pytest.raises(SharedRollback):
            await top_level_transaction.commit()

    assert "ROLLBACK" in caplog.text
    assert "COMMIT" not in caplog.text

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_shared_factory.py"
