# gcm-asyncdb

This library provides better, more robust database API with strong enforcement of transaction logic when working with
relational databases.

Currently, only MySQL/MariaDB (aiomysql) is supported, but support for other databases should be easy to implement, when required.

## Documentation

API documentation is available at https://gcm-cz.gitlab.io/gcm-asyncdb

## Features

- Fully type-safe, based on modern Python.
- Fully async.
- Connection pool with health checking and metrics.
- Strong transaction usage enforcement, automatic rollback.
- Advanced result fetching methods to simplify the usage.
- Observer support for transaction and query events.
- Structured logging with various loggers for various tasks, per-database logger, correct stack level.
- Exception hierarchy for correct and easier error handling.
- Timezone-aware datetimes (with possible fallback to naive datetimes).
- Native support for [sqlfactory](https://gitlab.com/gcm-cz/sqlfactory) statements.
- OpenTracing and OpenTelemetry support.

## Installation

```bash
pip install gcm-asyncdb
```

### Extras

- `opentracing` - for OpenTracing support
- `opentelemetry` - for OpenTelemetry support
- `sqlfactory` - for sqlfactory statement support
- `pydantic` - for pydantic model support in `fetch_object` calls.

## Usage

```python
from asyncdb.aiomysql import TransactionFactory, MySQLConfig

db = TransactionFactory(MySQLConfig(
    host='localhost',
    user='root',
    database='test'
))


async def main():
    async with db.transaction() as t:
        await t.query('CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))')
        await t.query('INSERT INTO test (id, name) VALUES (1, "test")')
        result = await t.query('SELECT * FROM test')
        async for row in result.fetch_all_dict():
            print(row)

        await t.commit()
```

## License

MIT

## Maturity

This library is still very new, but grew from multiple projects where it gradually evolved. So it is already used in
production, but it is still recommended to test it thoroughly before using it in production.

Historically, there has been problems with losing connections from connection pool in rare unpredictable error cases,
it seems to be resolved now, but there is no regression test for this, so be careful.

## Contributing

Feel free to open an issue or a pull request.
