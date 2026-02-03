from collections import defaultdict
from collections.abc import AsyncGenerator
from pprint import pprint
from typing import Any

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import Tracer

from asyncdb.aiomysql import MySQLConfig, TransactionFactory
from asyncdb.aiomysql.observers.opentelemetry_observer import OpenTelemetryTransactionObserver
from asyncdb.exceptions import QueryError


@pytest.fixture
def span_exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def tracer_provider(span_exporter: InMemorySpanExporter) -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(span_exporter))
    return provider


@pytest.fixture
def tracer(tracer_provider: TracerProvider) -> Tracer:
    return tracer_provider.get_tracer(__name__)


@pytest.fixture
def tracing_observer(tracer: Tracer) -> OpenTelemetryTransactionObserver:
    return OpenTelemetryTransactionObserver(tracer)


@pytest.fixture
async def factory(config: MySQLConfig, tracing_observer: OpenTelemetryTransactionObserver) -> AsyncGenerator[TransactionFactory]:
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
async def test_opentelemetry(factory: TransactionFactory, span_exporter: InMemorySpanExporter) -> None:
    async with factory.transaction() as trx:
        await trx.query("SELECT 1")
        await trx.commit()

    async with factory.transaction() as trx:
        await trx.query("SELECT 2")
        await trx.rollback()

    with pytest.raises(QueryError):
        async with factory.transaction() as trx:
            await trx.query("PARSE ERROR")

    spans = span_exporter.get_finished_spans()

    # Ensure all spans are finished.
    assert all(span.end_time is not None for span in spans)

    pprint(
        [
            {
                "name": span.name,
                "attributes": span.attributes,
                "span_id": span.context.span_id,
                "parent_id": span.parent.span_id if span.parent else None,
                "status": span.status.status_code if span.status else None,
            }
            for span in spans
        ]
    )

    span_tree: dict[int | None, dict[str, Any]] = defaultdict(lambda: {"children": []})

    for span in spans:
        span_tree[span.parent.span_id if span.parent else None]["children"].append(span_tree[span.context.span_id])

    for span in spans:
        span_tree[span.context.span_id]["span"] = span

    root_span = span_tree[None]

    assert len(root_span["children"]) == 3

    # Ensure all root spans are transactions.
    assert all(span["span"].name == "sql_transaction" for span in root_span["children"])

    # First transaction is begin, select, commit
    assert len(root_span["children"][0]["children"]) == 3
    assert [span["span"].name for span in root_span["children"][0]["children"]] == [
        "sql_query_begin",
        "sql_query_select",
        "sql_query_commit",
    ]
    assert [span["span"].attributes.get("db.query.text") for span in root_span["children"][0]["children"]] == [
        "BEGIN",
        "SELECT 1",
        "COMMIT",
    ]

    # Second transaction is begin, select, rollback
    assert len(root_span["children"][1]["children"]) == 3
    assert [span["span"].name for span in root_span["children"][1]["children"]] == [
        "sql_query_begin",
        "sql_query_select",
        "sql_query_rollback",
    ]
    assert [span["span"].attributes.get("db.query.text") for span in root_span["children"][1]["children"]] == [
        "BEGIN",
        "SELECT 2",
        "ROLLBACK",
    ]

    # Third transaction is begin, parse, rollback
    assert len(root_span["children"][2]["children"]) == 3
    assert [span["span"].name for span in root_span["children"][2]["children"]] == [
        "sql_query_begin",
        "sql_query_parse",
        "sql_query_rollback",
    ]
    assert [span["span"].attributes.get("db.query.text") for span in root_span["children"][2]["children"]] == [
        "BEGIN",
        "PARSE ERROR",
        "ROLLBACK",
    ]
    # Ensure the query that caused the error is tagged with error
    assert root_span["children"][2]["children"][1]["span"].status.status_code.name == "ERROR"
    assert "error.type" in root_span["children"][2]["children"][1]["span"].attributes
