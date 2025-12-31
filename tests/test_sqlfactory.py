import os
import warnings
from dataclasses import dataclass
from typing import Any

import pytest
from sqlfactory import Aliased, Select, UnionAll, Value

from asyncdb import ResultRow
from asyncdb.aiomysql import AioMySQLConnection, Transaction
from asyncdb.exceptions import ImplicitRollback, LogicError

from .conftest import CatchQueryObserver


@pytest.mark.asyncio
async def test_simple_query(transaction: Transaction, catch_queries: CatchQueryObserver) -> None:
    catch_queries.clear()
    q = await transaction.query(Select(Aliased(Value(1), alias="result")))
    async for row in q.fetch_all_dict():
        assert isinstance(row, dict)
        assert row["result"] == 1

    assert "SELECT 1 AS `result`" in catch_queries.QUERIES_EXECUTED
    await transaction.commit()


@pytest.mark.asyncio
async def test_rollback_when_exception(connection: AioMySQLConnection, catch_queries: CatchQueryObserver) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            with pytest.raises(TypeError):
                catch_queries.clear()
                await transaction.get_scalar(Select(Value(1)))
                raise TypeError("Simulated error after query")

    assert "SELECT 1" in catch_queries.QUERIES_EXECUTED
    assert "ROLLBACK" in catch_queries.QUERIES_EXECUTED
    assert "COMMIT" not in catch_queries.QUERIES_EXECUTED


@pytest.mark.asyncio
async def test_get(transaction: Transaction, caplog: pytest.LogCaptureFixture) -> None:
    assert await transaction.get_scalar(Select(Value(1))) == 1
    assert await transaction.get_list(Select(Value(1))) == [1]
    assert await transaction.get_dict(Select(Value(1))) == {"1": 1}
    await transaction.commit()

    for record in caplog.records:
        if "SELECT 1" in record.message:
            assert record.filename == "test_sqlfactory.py"


@pytest.mark.asyncio
async def test_operation_after_commit(connection: AioMySQLConnection) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")

        async with Transaction(connection) as transaction:
            await transaction.query(Select(Value(1)))
            await transaction.commit()

            with pytest.raises(LogicError):
                await transaction.query(Select(Value(1)))


@pytest.mark.asyncio
async def test_errors(connection: AioMySQLConnection, caplog: pytest.LogCaptureFixture) -> None:
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            q = await transaction.query(Select(Value(1)))

            with pytest.raises(LogicError):
                await transaction.query(Select(Value(2)))

            await q.close()

    assert any(record.name == "db.mysql.test" for record in caplog.records)
    for record in caplog.records:
        if record.name == "db.mysql.test":
            assert record.filename == "test_sqlfactory.py", record.message


@pytest.mark.asyncio
async def test_fetching_dict(connection: AioMySQLConnection) -> None:
    rows: list[dict[str, Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        async for row in q.fetch_all_dict():
            assert isinstance(row, dict)
            rows.append(row)

        await trx.commit()

    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


class CustomDict(dict[str, Any]):
    pass


@pytest.mark.asyncio
async def test_fetching_dict_custom(connection: AioMySQLConnection) -> None:
    rows: list[dict[str, Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        async for row in q.fetch_all_dict(CustomDict):
            assert isinstance(row, CustomDict)
            rows.append(row)

        await trx.commit()

    assert rows == [{"a": 1}, {"a": 2}, {"a": 3}]


@pytest.mark.asyncio
async def test_fetch_list(connection: AioMySQLConnection) -> None:
    rows: list[list[Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        async for row in q.fetch_all_list():
            assert isinstance(row, list)
            rows.append(row)

        await trx.commit()

    assert rows == [[1], [2], [3]]


class CustomList(list[Any]):
    pass


@pytest.mark.asyncio
async def test_fetch_list_custom(connection: AioMySQLConnection) -> None:
    rows: list[list[Any]] = []
    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        async for row in q.fetch_all_list(CustomList):
            assert isinstance(row, CustomList)
            rows.append(row)

        await trx.commit()

    assert rows == [[1], [2], [3]]


@pytest.mark.asyncio
async def test_fetch_object(connection: AioMySQLConnection) -> None:
    rows: list[ResultRow] = []

    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
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
class CustomObject:
    a: int


@pytest.mark.asyncio
async def test_fetch_custom_object(connection: AioMySQLConnection) -> None:
    rows: list[CustomObject] = []

    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        async for row in q.fetch_all_object(CustomObject, as_kwargs=True):
            assert isinstance(row, CustomObject)
            rows.append(row)

        await trx.commit()

    assert rows[0].a == 1
    assert rows[1].a == 2
    assert rows[2].a == 3


@pytest.mark.asyncio
async def test_fetch_scalar(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        q = await trx.query(
            UnionAll(
                Select(Aliased(Value(1), alias="a")), Select(Aliased(Value(2), alias="a")), Select(Aliased(Value(3), alias="a"))
            )
        )
        assert await q.fetch_all_scalar() == [1, 2, 3]
        await trx.commit()


@pytest.mark.asyncio
async def test_fetch_scalar_multiple_columns(connection: AioMySQLConnection) -> None:
    async with Transaction(connection) as trx:
        with pytest.raises(LogicError):
            await trx.get_scalar(Select(Value("a"), Value("b"), Value("c")))

        q = await trx.query(Select(Value("a"), Value("b"), Value("c")))
        with pytest.raises(LogicError):
            await q.fetch_all_scalar()

        await trx.commit()
