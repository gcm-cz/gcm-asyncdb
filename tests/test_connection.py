import os
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from asyncdb import LogicError, ResultRow, ObjectFetcher
from asyncdb.aiomysql import AioMySQLConnection, QueryObserver, Transaction, TransactionFactory, MySQLConfig, \
    TransactionIsolationLevel
from asyncdb.aiomysql.result import BoolResult
from asyncdb.exceptions import ImplicitRollback, ParseError, ScrollIndexError


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
def catch_queries():
    return TestQueryObserver()


@pytest.fixture(scope="function")
def connection(catch_queries):
    conn = AioMySQLConnection(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
    )
    conn.attach(catch_queries)
    return conn


@pytest.fixture(scope="function")
def factory(catch_queries):
    factory = TransactionFactory(
        config=config,
        connection_observers=[catch_queries],
    )
    yield factory
    TransactionFactory._pool_cache.clear()

@pytest.fixture(scope="function")
def connection_naive_dt():
    return AioMySQLConnection(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
        timezone=None,
    )


@pytest.mark.asyncio
async def test_simple_query(connection, catch_queries, caplog):
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

    for record in caplog.records:
        assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_rollback_when_exception(connection, catch_queries):
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
async def test_rollback_factory_when_exception(factory, catch_queries):
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
async def test_get(connection, caplog):
    async with Transaction(connection) as trx:
        assert await trx.get_scalar("SELECT 1") == 1
        assert await trx.get_list("SELECT 1") == [1]
        assert await trx.get_dict("SELECT 1") == {"1": 1}

    for record in caplog.records:
        if "SELECT 1" in record.message:
            assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_commit(connection, catch_queries):
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
async def test_operation_after_commit(connection):
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        async with Transaction(connection) as transaction:
            await transaction.query("SELECT 1")
            await transaction.commit()

            with pytest.raises(LogicError):
                await transaction.query("ANYTHING")


@pytest.mark.asyncio
async def test_errors(connection, caplog):
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            q = await transaction.query("SELECT 1")

            with pytest.raises(LogicError):
                await transaction.query("SELECT 2")

            await q.close()

            with pytest.raises(ParseError):
                await transaction.query("SYNTAX ERROR")

    print("\n".join(map(str, caplog.records)))

    for record in caplog.records:
        assert record.filename == "test_connection.py", record.message


@pytest.mark.asyncio
async def test_bool_result(connection, caplog):
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            q = await transaction.query("COMMIT")
            assert isinstance(q, BoolResult)
            assert bool(q) is True

    for record in caplog.records:
        assert record.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_naive_datetime(connection_naive_dt):
    async with Transaction(connection_naive_dt) as transaction:
        dt: datetime = await transaction.get_scalar("SELECT CAST('2021-01-03 13:15:16' AS DATETIME)")
        assert dt.year == 2021
        assert dt.month == 1
        assert dt.day == 3
        assert dt.hour == 13
        assert dt.minute == 15
        assert dt.second == 16
        assert dt.tzinfo is None


@pytest.mark.asyncio
async def test_aware_datetime(connection):
    async with Transaction(connection) as transaction:
        dt: datetime = await transaction.get_scalar("SELECT CAST('2021-01-03 13:15:16' AS DATETIME)")
        assert dt.year == 2021
        assert dt.month == 1
        assert dt.day == 3
        assert dt.hour == 13
        assert dt.minute == 15
        assert dt.second == 16
        assert dt.tzinfo is timezone.utc


@pytest.mark.asyncio
async def test_logging_correct(connection, caplog):
    async with Transaction(connection) as transaction:
        caplog.clear()

        q = await transaction.query("SELECT 1")
        await q.close()
        await transaction.commit()

    # Ensure all log records are logged from this test file, and not from the library code.
    for row in caplog.records:
        assert row.filename == "test_connection.py"


@pytest.mark.asyncio
async def test_fetching_dict(connection):
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
async def test_fetching_dict_custom(connection):
    rows: list[dict[str, Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_dict(customdict):
            assert isinstance(row, customdict)
            rows.append(row)

        await trx.commit()

    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.mark.asyncio
async def test_fetch_list(connection):
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
async def test_fetch_list_custom(connection):
    rows: list[list[Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION ALL SELECT 2 AS 'a' UNION ALL SELECT 3 AS 'a'")
        async for row in q.fetch_all_list(customlist):
            assert isinstance(row, customlist)
            rows.append(row)

        await trx.commit()

    assert rows == [[1], [2], [3]]


@pytest.mark.asyncio
async def test_fetch_object(connection):
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


@dataclass
class customobject:
    a: int


@pytest.mark.asyncio
async def test_fetch_custom_object(connection):
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
async def test_fetch_scalar(connection):
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1 AS 'a' UNION SELECT 2 AS 'a' UNION SELECT 3 AS 'a'")
        assert await q.fetch_all_scalar() == [1, 2, 3]
        await trx.commit()


@pytest.mark.asyncio
async def test_fetch_scalar_multiple_columns(connection):
    async with Transaction(connection) as trx:
        with pytest.raises(LogicError):
            await trx.get_scalar("SELECT 'a', 'b', 'c'")

        q = await trx.query("SELECT 'a', 'b', 'c'")
        with pytest.raises(LogicError):
            await q.fetch_all_scalar()

        await trx.commit()


@pytest.mark.asyncio
async def test_last_insert_id_affected_rows(connection):
    async with Transaction(connection) as trx:
        await trx.query("CREATE TEMPORARY TABLE test (id INT AUTO_INCREMENT PRIMARY KEY, a INT)")
        await trx.query("INSERT INTO test (a) VALUES (1)")
        assert await trx.last_insert_id() == 1

        await trx.query("INSERT INTO test (a) VALUES (2), (3)")

        await trx.query("UPDATE test SET a = 4")
        assert await trx.affected_rows() == 3

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_compatibility(connection):
    async with Transaction(connection) as trx:
        with pytest.warns(DeprecationWarning):
            await trx.execute("SELECT 1")
            assert await trx.fetchone() == {"1": 1}
            await trx.cleanup()

            await trx.execute("SELECT 1")
            assert await trx.fetchone() == {"1": 1}
            assert await trx.fetchone() == None

            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")
            assert await trx.fetchall() == [{"a": 1}, {"a": 2}]

            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")
            assert await trx.fetchmany(1) == [{"a": 1}]
            assert await trx.fetchmany(1) == [{"a": 2}]
            assert await trx.fetchmany(1) == []

            await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a`")
            (await trx.cursor).arraysize = 1000
            assert await trx.fetchmany() == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_dbapi_2_executemany(connection):
    async with Transaction(connection) as trx:
        await trx.execute("CREATE TEMPORARY TABLE test (a INT)")
        await trx.executemany("INSERT INTO test (a) VALUES (%s)", [(1,), (2,), (3,)])
        await trx.execute("SELECT * FROM test")
        await trx.fetchall() == [{"a": 1}, {"a": 2}, {"a": 3}]

        await trx.commit()


@pytest.mark.asyncio
async def test_dbapi_2_no_result(connection):
    async with Transaction(connection) as trx:
        await trx.execute("CREATE TEMPORARY TABLE test (a INT)")
        with pytest.raises(LogicError):
            await trx.fetchone()

        with pytest.raises(LogicError):
            await trx.fetchall()

        with pytest.raises(LogicError):
            await trx.fetchmany(1)

        with pytest.raises(LogicError):
            await trx.scroll(0)


@pytest.mark.asyncio
async def test_dbapi_2_scroll(connection):
    async with Transaction(connection) as trx:
        await trx.execute("SELECT 1 AS `a` UNION SELECT 2 AS `a` UNION SELECT 3 AS `a`")
        assert await trx.fetchone() == {"a": 1}
        assert await trx.fetchone() == {"a": 2}
        await trx.scroll(0, "absolute")
        assert await trx.fetchone() == {"a": 1}

        await trx.scroll(1, "absolute")
        assert await trx.fetchone() == {"a": 2}

        await trx.scroll(-1, "relative")
        assert await trx.fetchone() == {"a": 2}

        await trx.scroll(-2, "relative")
        assert await trx.fetchone() == {"a": 1}

        await trx.scroll(0, "relative")
        assert await trx.fetchone() == {"a": 2}

        await trx.scroll(0, "relative")
        assert await trx.fetchone() == {"a": 3}

        await trx.scroll(-3, "relative")
        assert await trx.fetchone() == {"a": 1}

        await trx.scroll(1, "relative")
        assert await trx.fetchone() == {"a": 3}

        await trx.scroll(-3, "relative")
        assert await trx.fetchone() == {"a": 1}

        with pytest.raises(IndexError):
            await trx.scroll(-4)

        with pytest.raises(ScrollIndexError):
            await trx.scroll(3)

        await trx.commit()


@pytest.mark.asyncio
async def test_objectfetcher(connection):
    @dataclass
    class Product:
        id: int
        name: str

    async def fetch_products(trx: Transaction) -> list[Product]:
        out: list[Product] = []

        q = await trx.query("SELECT `id`, `name` FROM `products`")
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
async def test_transaction_isolation_level(connection, caplog):
    async with Transaction(connection, isolation_level=TransactionIsolationLevel.SERIALIZABLE) as trx:
        await trx.commit()

    for record in caplog.records:
        # Ensure that the isolation level is set correctly and logged from where the transaction has been started.
        if "SET TRANSACTION ISOLATION LEVEL" in record.message:
            assert "SERIALIZABLE" in record.message
            assert record.filename == "test_connection.py"

        if "BEGIN" in record.message or "COMMIT" in record.message:
            assert record.filename == "test_connection.py"
