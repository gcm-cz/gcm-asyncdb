import asyncio
import os
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, AsyncGenerator

import pytest
from dateutil import tz

from asyncdb import LogicError, ObjectFetcher, ResultRow
from asyncdb.aiomysql import (
    AioMySQLConnection,
    Transaction,
    TransactionIsolationLevel,
    MySQLConfig,
    TransactionFactory,
)
from asyncdb.aiomysql.pool import transaction
from asyncdb.aiomysql.result import BoolResult
from asyncdb.exceptions import DuplicateEntry, EntityNotFound, Error, ImplicitRollback, ParseError, ScrollIndexError
from asyncdb.pool import transaction as generic_transaction

from .conftest import CatchQueryObserver


@pytest.fixture(scope="function")
async def connection_naive_dt(config: MySQLConfig) -> AsyncGenerator[AioMySQLConnection, None]:
    conn = AioMySQLConnection(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
        timezone=None,
    )

    try:
        yield conn
    finally:
        await conn.ensure_closed()


@pytest.mark.asyncio
async def test_simple_query(connection: AioMySQLConnection, catch_queries: CatchQueryObserver, caplog: pytest.LogCaptureFixture) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as trx:
            catch_queries.clear()
            q = await trx.query("SELECT 1")
            async for row in q.fetch_all_dict():
                assert isinstance(row, dict)
                assert row["1"] == 1

    assert "SELECT 1" in catch_queries.QUERIES_EXECUTED
    # Query has been implicitly rolled back, so there is ROLLBACK and no COMMIT in the executed queries.
    assert "ROLLBACK" in catch_queries.QUERIES_EXECUTED
    assert "COMMIT" not in catch_queries.QUERIES_EXECUTED

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_rollback_when_exception(connection: AioMySQLConnection, catch_queries: CatchQueryObserver) -> None:
    with pytest.raises(TypeError):
        with pytest.warns(ImplicitRollback):
            async with Transaction(connection) as trx:
                catch_queries.clear()
                one = await trx.get_scalar("SELECT 1")
                print(one / None)
                await trx.commit()

    assert "SELECT 1" in catch_queries.QUERIES_EXECUTED
    assert "ROLLBACK" in catch_queries.QUERIES_EXECUTED
    assert "COMMIT" not in catch_queries.QUERIES_EXECUTED


@pytest.mark.asyncio
async def test_rollback_factory_when_exception(factory: TransactionFactory, catch_queries: CatchQueryObserver) -> None:
    with pytest.raises(TypeError):
        with pytest.warns(ImplicitRollback):
            async with factory.transaction() as trx:
                catch_queries.clear()
                one = await trx.get_scalar("SELECT 1")
                print(one / None)
                await trx.commit()

    assert "SELECT 1" in catch_queries.QUERIES_EXECUTED
    assert "ROLLBACK" in catch_queries.QUERIES_EXECUTED
    assert "COMMIT" not in catch_queries.QUERIES_EXECUTED


@pytest.mark.asyncio
async def test_get(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    async with Transaction(connection) as trx:
        assert await trx.get_scalar("SELECT 1") == 1
        assert await trx.get_list("SELECT 1") == [1]
        assert await trx.get_dict("SELECT 1") == {"1": 1}
        await trx.commit()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if "SELECT 1" in record.message and record.name == "db.mysql.test":
            assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_commit(connection: AioMySQLConnection, catch_queries: CatchQueryObserver) -> None:
    async with Transaction(connection) as trx:
        catch_queries.clear()
        q = await trx.query("SELECT 1")
        async for row in q.fetch_all_list():
            assert isinstance(row, list)
            assert row[0] == 1
        await trx.commit()

    assert "ROLLBACK" not in catch_queries.QUERIES_EXECUTED
    assert "COMMIT" in catch_queries.QUERIES_EXECUTED


@pytest.mark.asyncio
async def test_operation_after_commit(connection: AioMySQLConnection) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        async with Transaction(connection) as transaction:
            await transaction.query("SELECT 1")
            await transaction.commit()

            with pytest.raises(LogicError):
                await transaction.query("ANYTHING")

@pytest.mark.asyncio
async def test_errors(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            q = await transaction.query("SELECT 1")

            with pytest.raises(LogicError):
                await transaction.query("SELECT 2")

            await q.close()

            with pytest.raises(ParseError):
                await transaction.query("SYNTAX ERROR")

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_connection.py", record.message


@pytest.mark.asyncio
async def test_bool_result_basic(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            q = await transaction.query("COMMIT")
            assert isinstance(q, BoolResult)
            assert bool(q) is True
            assert q == True
            assert q == q

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_bool_result_access(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        res = await trx.query("COMMIT")

        assert isinstance(res, BoolResult)
        assert bool(res) is True

        with pytest.raises(Error):
            async for row in res.fetch_all_dict():
                assert False, "Should not return anything!"

        with pytest.raises(Error):
            async for row in res.fetch_all_list():
                assert False, "Should not return anything!"

        with pytest.raises(Error):
            await res.fetch_all_scalar()

        await trx.commit()


@pytest.mark.asyncio
async def test_naive_datetime(connection_naive_dt: AioMySQLConnection) -> None:
    async with Transaction(connection_naive_dt) as transaction:
        dt: datetime = await transaction.get_scalar("SELECT CAST('2021-01-03 13:15:16' AS DATETIME)")
        assert dt.year == 2021
        assert dt.month == 1
        assert dt.day == 3
        assert dt.hour == 13
        assert dt.minute == 15
        assert dt.second == 16
        assert dt.tzinfo is None

        await transaction.commit()


@pytest.mark.asyncio
async def test_aware_datetime(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as transaction:
        dt: datetime = await transaction.get_scalar("SELECT CAST('2021-01-03 13:15:16' AS DATETIME)")
        assert dt.year == 2021
        assert dt.month == 1
        assert dt.day == 3
        assert dt.hour == 13
        assert dt.minute == 15
        assert dt.second == 16
        assert dt.tzinfo is timezone.utc

        await transaction.commit()


@pytest.mark.asyncio
async def test_aware_datetime_different_tz(connection: AioMySQLConnection) -> None:
    async with Transaction(connection, isolation_level=TransactionIsolationLevel.READ_COMMITTED) as transaction:
        dt = datetime(2021, 1, 3, 13, 15, 16, tzinfo=tz.gettz("US/Eastern"))  # UTC-5

        # Ensure the timezone of connection is UTC.
        assert transaction.connection.timezone == timezone.utc

        await transaction.query("CREATE TEMPORARY TABLE `test` (`dt` DATETIME, `ts` TIMESTAMP, `d` DATE, `t` TIME)")
        await transaction.query("INSERT INTO `test` (`dt`, `ts`, `d`, `t`) VALUES (%s, %s, %s, %s)", dt, dt, dt.date(), dt.timetz())  # Insert date in different timezone

        # Check whether the physically stored date is in UTC.
        assert await transaction.get_scalar("SELECT CONCAT('a', `dt`) FROM `test`") == 'a2021-01-03 18:15:16'
        assert await transaction.get_scalar("SELECT CONCAT('a', `ts`) FROM `test`") == "a2021-01-03 18:15:16"
        assert await transaction.get_scalar("SELECT CONCAT('a', `d`) FROM `test`") == "a2021-01-03"
        assert await transaction.get_scalar("SELECT CONCAT('a', `t`) FROM `test`") == "a13:15:16"

        # Try to fetch the date back.
        fetched_dt: datetime = await transaction.get_scalar("SELECT `dt` FROM `test`")
        assert fetched_dt == dt.astimezone(timezone.utc)
        assert fetched_dt.tzinfo == timezone.utc

        fetched_dt = await transaction.get_scalar("SELECT `ts` FROM `test`")
        assert fetched_dt == dt.astimezone(timezone.utc)
        assert fetched_dt.tzinfo == timezone.utc

        fetched_d: date = await transaction.get_scalar("SELECT `d` FROM `test`")
        assert fetched_d == dt.astimezone(timezone.utc).date()

        fetched_t: timedelta = await transaction.get_scalar("SELECT `t` FROM `test`")
        assert fetched_t.seconds == dt.hour * 3600 + dt.minute * 60 + dt.second

        await transaction.commit()


@pytest.mark.asyncio
async def test_datetime_ms(connection: AioMySQLConnection) -> None:
    async with Transaction(connection, isolation_level=TransactionIsolationLevel.READ_COMMITTED) as transaction:
        dt = datetime(2021, 1, 3, 13, 15, 16, 123456, tzinfo=timezone.utc)
        await transaction.query("CREATE TEMPORARY TABLE `test` (`dt` DATETIME, `dt_ms` DATETIME(6), `ts` TIMESTAMP, `ts_ms` TIMESTAMP(6))")
        await transaction.query("INSERT INTO `test` (`dt`, `dt_ms`, `ts`, `ts_ms`) VALUES (%s, %s, %s, %s)", dt, dt, dt, dt)

        assert await transaction.get_scalar("SELECT CONCAT('a', `dt`) FROM `test`") == 'a2021-01-03 13:15:16'
        assert await transaction.get_scalar("SELECT CONCAT('a', `dt_ms`) FROM `test`") == 'a2021-01-03 13:15:16.123456'
        assert await transaction.get_scalar("SELECT CONCAT('a', `ts`) FROM `test`") == 'a2021-01-03 13:15:16'
        assert await transaction.get_scalar("SELECT CONCAT('a', `ts_ms`) FROM `test`") == 'a2021-01-03 13:15:16.123456'

        fetched_dt: datetime = await transaction.get_scalar("SELECT `dt` FROM `test`")
        assert fetched_dt == dt.replace(microsecond=0)

        fetched_dt = await transaction.get_scalar("SELECT `dt_ms` FROM `test`")
        assert fetched_dt == dt

        fetched_dt: datetime = await transaction.get_scalar("SELECT `ts` FROM `test`")
        assert fetched_dt == dt.replace(microsecond=0)

        fetched_dt = await transaction.get_scalar("SELECT `ts_ms` FROM `test`")
        assert fetched_dt == dt

        await transaction.commit()


@pytest.mark.asyncio
async def test_logging_correct(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    async with Transaction(connection) as transaction:
        caplog.clear()

        q = await transaction.query("SELECT 1")
        await q.close()
        await transaction.commit()

    # Ensure all log records are logged from this test file, and not from the library code.
    assert any(record.name == "db.mysql.test" for record in caplog.records)
    assert len([record for record in caplog.records if record.name == "db.mysql.test"]) >= 3  # At least query start, end and commit

    for row in caplog.records:
        if row.name == "db.mysql.test":
            assert row.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_fetching_dict(connection: AioMySQLConnection) -> None:
    rows: list[dict[str, Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_dict():
            assert isinstance(row, dict)
            rows.append(row)

        await trx.commit()

    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


class customdict(dict):
    pass


@pytest.mark.asyncio
async def test_fetching_dict_custom(connection: AioMySQLConnection) -> None:
    rows: list[dict[str, Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_dict(customdict):
            assert isinstance(row, customdict)
            rows.append(row)

        await trx.commit()

    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.mark.asyncio
async def test_fetch_list(connection: AioMySQLConnection) -> None:
    rows: list[list[Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_list():
            assert isinstance(row, list)
            rows.append(row)

        await trx.commit()

    assert rows == [[1], [2], [3]]


class customlist(list):
    pass


@pytest.mark.asyncio
async def test_fetch_list_custom(connection: AioMySQLConnection) -> None:
    rows: list[list[Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_list(customlist):
            assert isinstance(row, customlist)
            rows.append(row)

        await trx.commit()

    assert rows == [[1], [2], [3]]


@pytest.mark.asyncio
async def test_fetch_object(connection: AioMySQLConnection) -> None:
    rows: list[ResultRow] = []

    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_object():
            assert isinstance(row, ResultRow)
            rows.append(row)

        await trx.commit()

    assert rows[0].a == 1
    assert rows[1].a == 2
    assert rows[2].a == 3

    # Access to non-existing column returns None.
    assert rows[0].b is None

    assert repr(rows[0]) == "<ResultRow a=1>"


@dataclass
class customobject:
    a: int


@pytest.mark.asyncio
async def test_fetch_custom_object(connection: AioMySQLConnection) -> None:
    rows: list[customobject] = []

    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_object(customobject, as_kwargs=True):
            assert isinstance(row, customobject)
            rows.append(row)

        await trx.commit()

    assert rows[0].a == 1
    assert rows[1].a == 2
    assert rows[2].a == 3


@pytest.mark.asyncio
async def test_fetch_scalar(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION SELECT 2 AS 'a' UNION SELECT 3 AS 'a'")
        assert await q.fetch_all_scalar() == [1, 2, 3]
        await trx.commit()


@pytest.mark.asyncio
async def test_fetch_scalar_multiple_columns(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.raises(LogicError):
            await trx.get_scalar("SELECT 'a', 'b', 'c'")

        q = await trx.query("SELECT 'a', 'b', 'c'")
        with pytest.raises(LogicError):
            await q.fetch_all_scalar()

        await trx.commit()


@pytest.mark.asyncio
async def test_last_insert_id_affected_rows(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        await trx.query("CREATE TEMPORARY TABLE test (id INT AUTO_INCREMENT PRIMARY KEY, a INT)")
        await trx.query("INSERT INTO test (a) VALUES (1)")
        assert await trx.last_insert_id() == 1

        await trx.query("INSERT INTO test (a) VALUES (2), (3)")

        await trx.query("UPDATE test SET a = 4")
        assert await trx.affected_rows() == 3

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_compatibility(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"1": 1}

        await trx.cleanup()

        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"1": 1}

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == None

        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchall() == [{"a": 1}, {"a": 2}]

        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchmany(1) == [{"a": 1}]

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchmany(1) == [{"a": 2}]

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchmany(1) == []

        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")

        with pytest.warns(DeprecationWarning):
            (await trx.cursor).arraysize = 1000

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchmany() == [{"a": 1}, {"a": 2}]

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_executemany(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.warns(DeprecationWarning):
            await trx.execute("CREATE TEMPORARY TABLE test (a INT)")

        with pytest.warns(DeprecationWarning):
            await trx.executemany("INSERT INTO test (a) VALUES (%s)", [(1,), (2,), (3,)])

        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT * FROM test")

        with pytest.warns(DeprecationWarning):
            await trx.fetchall() == [{"a": 1}, {"a": 2}, {"a": 3}]

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_no_result(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.warns(DeprecationWarning):
            await trx.execute("CREATE TEMPORARY TABLE test (a INT)")

        with pytest.raises(LogicError):
            with pytest.warns(DeprecationWarning):
                await trx.fetchone()

        with pytest.raises(LogicError):
            with pytest.warns(DeprecationWarning):
                await trx.fetchall()

        with pytest.raises(LogicError):
            with pytest.warns(DeprecationWarning):
                await trx.fetchmany(1)

        with pytest.raises(LogicError):
            with pytest.warns(DeprecationWarning):
                await trx.scroll(0)

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_scroll(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a` UNION SELECT 3 AS `a`")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 1}

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 2}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(0, "absolute")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 1}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(1, "absolute")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 2}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(-1, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 2}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(-2, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 1}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(0, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 2}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(0, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 3}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(-3, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 1}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(1, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 3}

        with pytest.warns(DeprecationWarning):
            await trx.scroll(-3, "relative")

        with pytest.warns(DeprecationWarning):
            assert await trx.fetchone() == {"a": 1}

        with pytest.raises(IndexError):
            with pytest.warns(DeprecationWarning):
                await trx.scroll(-4)

        with pytest.raises(ScrollIndexError):
            with pytest.warns(DeprecationWarning):
                await trx.scroll(3)

        await trx.commit()


@pytest.mark.asyncio
async def test_objectfetcher(connection: AioMySQLConnection) -> None:
    @dataclass
    class Product:
        id: int
        name: str

    async def fetch_products(trx: Transaction) -> list[Product]:
        out: list[Product] = []

        q = await trx.query("SELECT `id`, `name` FROM `products`")

        with pytest.warns(DeprecationWarning):
            async for product in ObjectFetcher[Product](q, Product):
                out.append(product)

        return out

    async with Transaction(connection) as trx:
        await trx.query("CREATE TEMPORARY TABLE products (id INT PRIMARY KEY, name VARCHAR(255))")
        await trx.query("INSERT INTO products (id, name) VALUES (1, 'Product 1'), (2, 'Product 2'), (3, 'Product 3')")

        assert await fetch_products(trx) == [
            Product(id=1, name="Product 1"),
            Product(id=2, name="Product 2"),
            Product(id=3, name="Product 3"),
        ]

        await trx.commit()

@pytest.mark.asyncio
async def test_transaction_isolation_level(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    async with Transaction(connection, isolation_level=TransactionIsolationLevel.SERIALIZABLE) as trx:
        await trx.commit()

    for record in caplog.records:
        # Ensure that the isolation level is set correctly and logged from where the transaction has been started.
        if "SET TRANSACTION ISOLATION LEVEL" in record.message:
            assert "SERIALIZABLE" in record.message
            assert record.filename == "test_connection.py"

        if "BEGIN" in record.message or "COMMIT" in record.message:
            assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_query_with_unknown_args(connection: AioMySQLConnection) -> None:
    with pytest.raises(TypeError):
        with pytest.warns(ImplicitRollback):
            async with Transaction(connection) as trx:
                await trx.query("SELECT %s")


@pytest.mark.asyncio
async def test_factory_call(factory: TransactionFactory) -> None:
    gen = factory()
    try:
        async for trx in gen:
            assert isinstance(trx, Transaction)
            await trx.commit()
            break
        else:
            raise AssertionError("No transaction yielded from factory.")
    finally:
        await gen.aclose()


@pytest.mark.asyncio
async def test_transaction_function(factory: TransactionFactory) -> None:
    async with transaction(factory.pool) as trx:
        assert isinstance(trx, Transaction)
        await trx.commit()


@pytest.mark.asyncio
async def test_generic_transaction_function(factory: TransactionFactory) -> None:
    async with generic_transaction(factory.pool) as trx:
        assert isinstance(trx, Transaction)
        await trx.commit()


@pytest.mark.asyncio
async def test_iterate_result_set(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        res = await trx.query("SELECT 1; SELECT 1;")
        assert await res.fetch_all_scalar() == [1, 1]
        await trx.commit()


@pytest.mark.asyncio
async def test_discard_result(connection: AioMySQLConnection):
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as trx:
            res = await trx.query("SELECT 1; SELECT 1;")
            await res.close()

            await trx.query("SELECT 1")  # Should not raise exception


@pytest.mark.asyncio
async def test_result_not_discarded(connection: AioMySQLConnection) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as trx:
            res = await trx.query("SELECT 1; SELECT 1;")

            with pytest.raises(LogicError):
                await trx.query("SELECT 1")

            await res.close()

            await trx.query("SELECT 1")


@pytest.mark.asyncio
async def test_entity_not_found(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        await trx.query("CREATE TEMPORARY TABLE `test` (`id` INT PRIMARY KEY, `name` VARCHAR(255))")
        await trx.query("INSERT INTO `test` (`id`, `name`) VALUES (1, 'Test')")

        with pytest.raises(EntityNotFound):
            await trx.get_scalar("SELECT `id` FROM `test` WHERE `id` = 3")

        with pytest.raises(EntityNotFound):
            await trx.get_dict("SELECT `id` FROM `test` WHERE `id` = 3")

        with pytest.raises(EntityNotFound):
            await trx.get_list("SELECT `id` FROM `test` WHERE `id` = 3")

        with pytest.raises(EntityNotFound):
            await trx.get_object("SELECT `id` FROM `test` WHERE `id` = 3")

        await trx.commit()



@pytest.mark.asyncio
async def test_duplicate_entry(connection: AioMySQLConnection) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as trx:
            await trx.query("CREATE TEMPORARY TABLE `test` (`id` INT PRIMARY KEY, `name` VARCHAR(255))")
            await trx.query("INSERT INTO `test` (`id`, `name`) VALUES (1, 'Test')")

            with pytest.raises(DuplicateEntry):
                await trx.query("INSERT INTO `test` (`id`, `name`) VALUES (1, 'Test')")
