import warnings

from .mock import MockConnectionWrapper  # MUST BE BEFORE asyncdb IMPORTS!!!

import pytest
from pymysql import ProgrammingError
from pymysql.constants import ER

from asyncdb import LogicError
from asyncdb.aiomysql import AioMySQLConnection, Transaction
from asyncdb.exceptions import ImplicitRollback, SyntaxError


@pytest.fixture
def connection():
    return AioMySQLConnection()


@pytest.mark.asyncio
async def test_simple_query(connection):
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as trx:
            q = await trx.query("SELECT 1")
            async for row in q.fetch_all_dict():
                assert isinstance(row, dict)
                assert row["1"] == 1

    assert "ROLLBACK" in connection.QUERIES_EXECUTED
    assert "COMMIT" not in connection.QUERIES_EXECUTED


@pytest.mark.asyncio
async def test_get(connection):
    async with Transaction(connection) as trx:
        assert await trx.get_scalar("SELECT 1") == 1
        assert await trx.get_list("SELECT 1") == [1]
        assert await trx.get_dict("SELECT 1") == {"1": 1}


@pytest.mark.asyncio
async def test_commit(connection):
    async with Transaction(connection) as trx:
        q = await trx.query("SELECT 1")
        async for row in q.fetch_all_list():
            assert isinstance(row, list)
            assert row[0] == 1
        await trx.commit()

    assert "ROLLBACK" not in connection.QUERIES_EXECUTED
    assert "COMMIT" in connection.QUERIES_EXECUTED


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
async def test_errors(connection):
    with pytest.warns(ImplicitRollback):
        async with Transaction(connection) as transaction:
            connection.QUERIES["SYNTAX ERROR"] = ProgrammingError(ER.SYNTAX_ERROR, "Mocked syntax error")

            q = await transaction.query("SELECT 1")

            with pytest.raises(LogicError):
                await transaction.query("SELECT 2")

            await q.close()

            with pytest.raises(SyntaxError):
                await transaction.query("SYNTAX ERROR")
