"""
Alternative approach to database API with full native async support and advanced configurable connection pool.

Fully type-safe. For Python 3.11+.

Currently supported backend connectors:

- aiomysql

## Why asyncdb and not standard DB-API 2.0?

- Fully type-safe, based on modern Python.
- Fully async.
- Connection pool with health checking and metrics.
- Strong transaction usage enforcement, automatic rollback.
- Advanced result fetching methods to simplify the usage.
- Observer support for transaction and query events.
- Structured logging with various loggers for various tasks, per-database logger, correct stack level.
- Exception hierarchy for correct and easier error handling.
- Timezone-aware datetimes (with possible fallback to naive datetimes).

## Installation and basic usage

```bash
pip install gcm-asyncdb
```

```python
from asyncdb.aiomysql import TransactionFactory, MySQLConfig

db = TransactionFactory(MySQLConfig(
    host='localhost',
    user='root',
    database='test',
))

async def main() -> None:
    # TransactionFactory class selects connection from connection pool and opens a new transaction on top of it.
    # Transaction is the main access port to the database, all operations are done on top of it.
    # For aiomysql, you can even specify transaction isolation level for the transaction. See API docs for more info.
    async with db.transaction() as trx:
        # Instead of DB-API 2.0 cursor' execute, there is a query() method, which returns Result object, that is then
        # used to fetch the results.
        await trx.query('CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(255))')
        await trx.query('INSERT INTO test (id, name) VALUES (1, "test")')

        # Let's see some query that fetches rows...
        result = await trx.query('SELECT * FROM test')

        # Iterate over the rows in result set. There are multiple ways how to fetch the result, see API docs.
        async for row in result.fetch_all_dict():
            # Row is a dictionary with column names as keys.
            print(row)

        # The transaction will be automatically rolled back if it is not commited manually. This is a safety feature
        # to prevent accidental commit of transaction that is not completed properly.
        await trx.commit()
```

## Retrieving results

### Single-row result

Often, you need just one row from the database. Instead of rather complex execute() and fetchone() calls from DB-API 2.0,
in asyncdb, you can use row getters:

- `Transaction.get_object()`
- `Transaction.get_dict()`
- `Transaction.get_list()`
- `Transaction.get_scalar()`

If the query returned no rows, `EntityNotFound` exception is automatically raised. If the query returned more than one row,
`TooManyRowsError` exception is raised.

### Multi-row results

For multiple rows, you can use `Result` object, that is returned from `Transaction.query()` method. It has multiple
methods to fetch the results:

- `Result.fetch_all_object()`
- `Result.fetch_all_dict()`
- `Result.fetch_all_list()`
- `Result.fetch_all_scalar()`

First three methors return async generator that yields rows in the result set. The last one returns a list of scalars for
all rows in the result set.

### Advanced usage

All the `fetch_*` and `get_*` methods (except `get_scalar()` and `fetch_all_scalar()` obviously) accepts factory function
to customize the result object creation. This is useful when you want to create custom object from the row, or you want
to create a different structure than `list` or `dict`.

## Connection pool

For async applications, connection pool is a must. It is a collection of database connections that are ready to be used
for handling transactions. Each connection in the pool is regularly checked for health and if it is not healthy, it is
automatically replaced.

Connection pool is created automatically when the first transaction is created. You can also create the pool manually
using `TransactionFactory.ensure_pool()` method.

The connection pool can be configured using `MySQLConfig` object:

- `max_pool_size` - maximum number of connections in the pool, including currently used ones.
- `max_spare_conns` - maximum number of spare connections in the pool always ready to be used. When there is peak in traffic
    and new connections are spawned, the pool can grow up to `max_pool_size` limit. When the traffic is low, the pool
    shrinks to `max_spare_conns` connections. When another connection is being returned to the pool, it is then discarded to
    keep the resource usage lower.
- `min_spare_conns` - minimum number of spare connections in the pool. If the pool has currently less available connections
    than this number, new connections are automatically created up to the `max_pool_size` limit.
- `max_conn_lifetime` - maximum lifetime of a connection in the pool in seconds. When the connection is older than this
    limit, it is automatically replaced with a new one.
- `max_conn_usage` - maximum number of transactions that can be handled by a single connection. When this limit is reached,
    the connection is automatically replaced with a new one. This is useful to prevent memory leaks historically present
    in some database drivers.

## Error handling

There are multiple error classes that are used to signal various errors in the database. All of them are subclasses of
`Error` class. The most common errors are:

- `Error` - base error class for database exceptions.
    - `ConnectError` - connection related errors.
    - `QueryError` - errors based on query execution. It then has
        - `LogicError` - logical error, for example trying to execute another query while previous result set still has not been
            fetched.
            - `TooManyRowsError` - too many rows returned where expected only one row.
            - `IntegrityError` - data integrity error such as foreign key violation or duplicate key.
                - `DuplicateEntry` - duplicate entry exception.
            - `EntityNotFound` - requested entity was not found
        - `ParseError` - SQL parsing has failed
        - `SyntaxError` - SQL parsing succeeded, but the syntax is incorrect
        - `Deadlock` - database deadlocked.
        - `LostConnection` - connection to the database was lost during query.
        - `LockWaitTimeout` - lock wait timeout exceeded while trying to get lock for the rows.
        - `ServerGone` - connection died before the query could be processed.
        - `QueryTooBig` - SQL query exceeded the maximum allowed size.
- `ImplicitRollback` - warning type issued when transaction has been automatically rolled back at the exit of the context.

All the exceptions contain optional `remote_app` property which identifies the connection that caused the error.

## Logging

The library uses structured logging with various loggers for various tasks. The main logger is `db` and it is used as a base
for all other loggers. The loggers are:

- `db.pool.health` - logs health checks of the connection pool.
- `db.mysql.startup` - logs MySQL connection startup.
- `db.mysql.<database>` - logger for specific database, useful when application works with various databases to identify
    the source of the log message.

## Examples

### Load objects from database

To fetch rows from database, use `Result.fetch_all_object()`.

```python
trx: Transaction = ...
res = await trx.query("SELECT id, name, price FROM products")

async for row in res.fetch_all_object():
    print(f"ID: {row.id}, name: {row.name}, price: {row.price}")
```

When you want just one object, you can use `Transaction.get_object()`.

```python
trx: Transaction
row = await trx.get_boject("SELECT id, name, price FROM products WHERE id = %s", [1])
print(f"Product ID {row.id}: {row.name}")
```

### Load dictionaries from database

```python
trx: Transaction = ...
res = await trx.query("SELECT id, name, price FROM products")

async for row in res.fetch_all_dict():
    print(f"ID: {row['id']}, name: {row['name']}, price: {row['price']}")
```

You can also retrieve one object from the database

```python
trx: Transaction = ...
row = await trx.get_dict("SELECT id, name, price FROM products WHERE id = %s", [1])
print(f"Product ID {row['id']}: {row['name']}
```

### Load lists from database

```python
trx: Transaction = ...
res = await trx.query("SELECT COUNT(*), SUM(price) FROM products")

async for row in res.fetch_all_list():
    print(f"Number of products: {row[0]}, total price: {row[1]}")
```

Also, single-row version also works:

```python
trx: Transaction = ...
row = await trx.get_list("SELECT COUNT(*), SUM(price) FROM products")
print(f"Number of products: {row[0]}, total price: {row[1]}")
```

### Simply load set of existing IDs

```python
trx: Transaction = ...
res = await trx.query("SELECT id FROM products")

# You can specify factory function as the argument to the
# `Transaction.fetch_all_scalar()` to provide custom container
# for the result set.
ids = await res.fetch_all_scalar(set)

print(ids)  # set(1,2,3)

```

### Load single value from database

```python
trx: Transaction = ...
count = await trx.get_scalar("SELECT COUNT(*) FROM products")
print(f"Number of products: {count}")
```

### Modify data

```python
trx: Transaction = ...
await trx.query("INSERT INTO products (name, price) VALUES (%s, %s)", "Product 1", 100)
print(f"ID of inserted entry: {await trx.last_insert_id()}")
```

```python
trx: Transaction = ...
await trx.query("UPDATE products SET price = price * 1.1 WHERE price < %s", 100)
print(f"Number of updated entries: {await trx.affected_rows()}")
```

### Modify transaction isolation level

To set transaction isolation level for the transaction, you can either specify

```python
# Default isolation level for transactions.
db = TransactionFactory(isolation_level=TransactionIsolationLevel.REPEATABLE_READ)

# Explicit isolation level for the transaction.
async with db.transaction(TransactionIsolationLevel.READ_COMMITTED) as trx:
    await trx.get_scalar("SELECT COUNT(*) FROM products")  # 0
    await trx.query("INSERT INTO products (name, price) VALUES (%s, %s)", "Product 1", 100)
    await trx.get_scalar("SELECT COUNT(*) FROM products")  # 1
    await trx.commit()
```

"""

from logging import getLogger

from .asyncdb import ObjectFetcher, Result, Transaction
from .context import TransactionContext
from .exceptions import EntityNotFound, LogicError, TooManyRowsError
from .generics import ResultRow

logger = getLogger("db")
"""Base logger for the database subsystem."""


__all__ = [  # noqa: RUF022
    "EntityNotFound",
    "LogicError",
    "ObjectFetcher",
    "Result",
    "ResultRow",
    "TooManyRowsError",
    "Transaction",
    "TransactionContext",
    "logger",
    # submodules
    "aiomysql",
    "exceptions",
    "pool",
]
