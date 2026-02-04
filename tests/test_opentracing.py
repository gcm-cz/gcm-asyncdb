from collections import defaultdict
from collections.abc import AsyncGenerator
from pprint import pprint
from typing import Any

import pytest
from opentracing.mocktracer import MockTracer
from opentracing.mocktracer.span import MockSpan
from opentracing.scope_managers.asyncio import AsyncioScopeManager

from asyncdb.aiomysql import MySQLConfig, TransactionFactory
from asyncdb.asyncdb.observers.opentracing_observer import OpentracingObserver
from asyncdb.exceptions import ImplicitRollback, QueryError


@pytest.fixture
def tracer() -> MockTracer:
    return MockTracer(AsyncioScopeManager())


@pytest.fixture
def tracing_observer(tracer: MockTracer) -> OpentracingObserver:
    return OpentracingObserver(tracer)


@pytest.fixture
async def factory(config: MySQLConfig, tracing_observer: OpentracingObserver) -> AsyncGenerator[TransactionFactory]:
    factory = TransactionFactory(config, transaction_observers=[tracing_observer])
    factory.ensure_pool(keeper_task=False)
    await factory.pool._keeper_task_iteration()

    try:
        yield factory
    finally:
        for pool in TransactionFactory._pool_cache.values():
            await pool.close()

        TransactionFactory._pool_cache.clear()


@pytest.mark.asyncio
async def test_opentracing(factory: TransactionFactory, tracer: MockTracer) -> None:
    async with factory.transaction() as trx:
        await trx.query("SELECT 1")
        await trx.commit()

    async with factory.transaction() as trx:
        await trx.query("SELECT 2")
        await trx.rollback()

    with pytest.raises(QueryError):
        with pytest.warns(ImplicitRollback):
            async with factory.transaction() as trx:
                await trx.query("PARSE ERROR")

    spans: list[MockSpan] = tracer.finished_spans()

    # Ensure all spans are finished.
    assert all(span.finished is True for span in spans)

    pprint(
        [
            {
                "operation_name": span.operation_name,
                "tags": span.tags,
                "span_id": span.context.span_id,
                "parent_id": span.parent_id,
                "finished": span.finished,
            }
            for span in spans
        ]
    )

    span_tree: dict[int | None, dict[str, Any]] = defaultdict(lambda: {"children": []})

    for span in spans:
        span_tree[span.parent_id]["children"].append(span_tree[span.context.span_id])

    for span in spans:
        span_tree[span.context.span_id]["span"] = span

    root_span = span_tree[None]

    assert len(root_span["children"]) == 3

    # Ensure all root spans are transactions.
    assert all(span["span"].operation_name == "sql_transaction" for span in root_span["children"])

    # First transaction is begin, select, commit
    assert len(root_span["children"][0]["children"]) == 3
    assert [span["span"].operation_name for span in root_span["children"][0]["children"]] == [
        "sql_query_begin",
        "sql_query_select",
        "sql_query_commit",
    ]
    assert [span["span"].tags["query"] for span in root_span["children"][0]["children"]] == [
        "BEGIN",
        "SELECT 1",
        "COMMIT",
    ]

    # Second transaction is begin, select, rollback
    assert len(root_span["children"][1]["children"]) == 3
    assert [span["span"].operation_name for span in root_span["children"][1]["children"]] == [
        "sql_query_begin",
        "sql_query_select",
        "sql_query_rollback",
    ]
    assert [span["span"].tags["query"] for span in root_span["children"][1]["children"]] == [
        "BEGIN",
        "SELECT 2",
        "ROLLBACK",
    ]

    # Third transaction is begin, parse, rollback
    assert len(root_span["children"][2]["children"]) == 3
    assert [span["span"].operation_name for span in root_span["children"][2]["children"]] == [
        "sql_query_begin",
        "sql_query_parse",
        "sql_query_rollback",
    ]
    assert [span["span"].tags["query"] for span in root_span["children"][2]["children"]] == [
        "BEGIN",
        "PARSE ERROR",
        "ROLLBACK",
    ]
    # Ensure the query that caused the error is tagged with error=True
    assert root_span["children"][2]["children"][1]["span"].tags["error"] is True
    assert "error.kind" in root_span["children"][2]["children"][1]["span"].tags
