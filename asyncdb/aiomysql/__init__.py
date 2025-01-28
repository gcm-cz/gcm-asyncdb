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

from .config import MySQLConfig, MySQLConfigProtocol
from .connection import AioMySQLConnection
from .factory import TransactionFactory
from .observer import QueryObserver, TransactionObserver
from .pool import MySQLConnectionPool
from .result import Result
from .transaction import Transaction, TransactionIsolationLevel

__all__ = [
    "AioMySQLConnection",
    "MySQLConfig",
    "MySQLConfigProtocol",
    "MySQLConnectionPool",
    "QueryObserver",
    "Result",
    "Transaction",
    "TransactionFactory",
    "TransactionIsolationLevel",
    "TransactionObserver",
]
