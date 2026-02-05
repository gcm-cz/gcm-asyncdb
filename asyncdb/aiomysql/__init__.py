"""
Async MySQL interface using the `aiomysql` driver.

Example:

```python
from asyncdb.aiomysql import MySQLConfig, TransactionFactory

config = MySQLConfig(...)
db = TransactionFactory(config)  # Initializes the database connection pool using configuration from `MySQLConfig`.

# Retrieve one connection from the connection pool and initialize a fresh transaction on it.
async with db.transaction() as trx:
    # Perform normal operations on the transaction.
    res = await trx.query("SELECT `id`, `name` FROM `products`")
    async for product in res.fetch_all_dict():
        print(product)

    # Don't forget to commit the transaction.
    await trx.commit()

# After the context exit, connection is returned to the pool and reused for other transactions.
```

"""

from asyncdb.aiomysql import observer as _observer
from asyncdb.aiomysql.config import MySQLConfig, MySQLConfigProtocol
from asyncdb.aiomysql.connection import AioMySQLConnection
from asyncdb.aiomysql.factory import TransactionFactory
from asyncdb.aiomysql.pool import MySQLConnectionPool
from asyncdb.aiomysql.result import Result
from asyncdb.aiomysql.shared_factory import SharedTransactionFactory
from asyncdb.aiomysql.transaction import SharedRollbackMode, Transaction, TransactionIsolationLevel

__all__ = [
    "AioMySQLConnection",
    "MySQLConfig",
    "MySQLConfigProtocol",
    "MySQLConnectionPool",
    "QueryObserver",  # pylint: disable=undefined-all-variable
    "Result",
    "SharedRollbackMode",
    "SharedTransactionFactory",
    "Transaction",
    "TransactionFactory",
    "TransactionIsolationLevel",
    "TransactionObserver",  # pylint: disable=undefined-all-variable
]


def __getattr__(name: str) -> object:
    if name in {"QueryObserver", "TransactionObserver"}:
        return getattr(_observer, name)

    raise AttributeError(f"module {__name__} has no attribute {name}")
