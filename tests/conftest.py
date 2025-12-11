import os
from typing import Any, AsyncGenerator

import pytest

from asyncdb.aiomysql import MySQLConfig, QueryObserver, AioMySQLConnection, TransactionFactory, MySQLConnectionPool, Transaction


@pytest.fixture
def config() -> MySQLConfig:
    return MySQLConfig(
        host=os.getenv("TEST_MYSQL_HOST", "localhost"),
        port=int(os.getenv("TEST_MYSQL_PORT", "3306")),
        user=os.getenv("TEST_MYSQL_USER", "root"),
        password=os.getenv("TEST_MYSQL_PASSWORD", ""),
        database=os.getenv("TEST_MYSQL_DATABASE", "test"),
        max_pool_size=1,
    )


class CatchQueryObserver(QueryObserver):
    def __init__(self):
        super().__init__()
        self.QUERIES_EXECUTED = []

    def observe_query_before(self, conn: AioMySQLConnection, query: str, **kwargs: Any) -> Any:
        self.QUERIES_EXECUTED.append(query)

    def clear(self):
        self.QUERIES_EXECUTED.clear()


@pytest.fixture(scope="function")
def catch_queries() -> CatchQueryObserver:
    return CatchQueryObserver()


@pytest.fixture(scope="function")
async def factory(catch_queries: CatchQueryObserver, config: MySQLConfig) -> AsyncGenerator[TransactionFactory, None]:
    factory = TransactionFactory(
        config=config,
        connection_observers=[catch_queries],
    )
    factory.ensure_pool(keeper_task=False)

    await factory.pool._keeper_task_iteration()

    try:
        yield factory

    finally:
        for pool in TransactionFactory._pool_cache.values():
            await pool.close()

        TransactionFactory._pool_cache.clear()


@pytest.fixture
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
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture(scope="function")
async def connection(config: MySQLConfig, catch_queries: CatchQueryObserver) -> AsyncGenerator[AioMySQLConnection, None]:
    conn = AioMySQLConnection(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        db=config.database,
    )
    conn.attach(catch_queries)

    try:
        yield conn
    finally:
        await conn.ensure_closed()


@pytest.fixture(scope="function")
async def transaction(connection: AioMySQLConnection) -> AsyncGenerator[Transaction, None]:
    async with Transaction(connection) as trx:
        yield trx
