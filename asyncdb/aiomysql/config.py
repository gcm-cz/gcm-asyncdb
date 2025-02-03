from dataclasses import dataclass
from datetime import timezone, tzinfo
from typing import Optional, Protocol


class MySQLConfigProtocol(Protocol):
    """
    Protocol for MySQL configuration to provide correct type hints. You can use any class that implements this protocol
    as a configuration for MySQL connection.
    """

    host: str
    """MySQL server address."""

    port: int
    """ MySQL server port."""

    user: str
    """ User to authenticate agains the database. """

    password: str
    """ Password for the user. """

    database: str
    """ Database name to use. """

    charset: str
    """ Charset to use for the connection. """

    max_pool_size: int
    """ Maximum number of connections in the pool including used ones. """

    max_spare_conns: int
    """
    Maximum number of spare connections in the pool. Under heavy load, the pool can grow up to max_pool_size. After the
    load decreases, the pool will shrink again, but will keep `max_spare_conns` still ready for next load peak.

    When the connections time out (either on usage or max lifetime), they are closed and new ones are not automatically created,
    so the pool shrinks naturally again.
    """

    min_spare_conns: int
    """ Minimum spare connections to keep in the pool ready to use. Up to `max_pool_size` connections can be created. """

    max_conn_lifetime: int
    """
    Maximum lifetime of a connection in seconds. After this time, the connection is discarded and potentially replaced with new
    one when needed.
    """

    max_conn_usage: int
    """
    Maximum number of transactions this connection can handle. After exceeding the usage count, the connection is discarded
    and potentially replaced with new one when needed.
    """

    connect_timeout: int
    """
    Timeout (in seconds) for the connection to be established. If the connection is not established in this time, the
    connection is considered failed and new attempt will be made.

    Also, this is a timeout used while waiting for connection to use for transaction, if no explicit timeout is specified
    in the transaction call.
    """

    read_timeout: int
    """
    Timeout (in seconds) for the query to be executed. If the query is not completed within this time frame, TimeoutError
    is raised.
    """

    write_timeout: int
    """
    Timeout (in seconds) for the query to be transfered from client to server. When the transfer time is longer, the query
    is failed and connection closed from the side of MySQL server.
    """

    wait_timeout: Optional[int] = None
    """
    Timeout (in seconds) for the connection to be idle before it is closed by the server.
    """

    remote_app: Optional[str] = None
    """
    Name of the connection to the database. This is used for logging purposes to distinguish between different connections.

    `remote_app` is propagated to exceptions.
    """

    timezone: Optional[tzinfo] = timezone.utc
    """
    Timezone to use for the connection. When specified, the database will expect all datetime values to be in this timezone
    and will return all datetime values in this timezone as timezone-aware datetimes.

    When timezone is None, the database will produce naive datetime instances and timezone handling is up to the application.
    In this mode, the connector will act as standard aiomysql connector, so it is useful for legacy applications which does not
    use timezone-aware datetimes within the code.
    """


@dataclass
class MySQLConfig(MySQLConfigProtocol):  # pylint: disable=too-many-instance-attributes
    """
    MySQL connection configuration dataclass, to provide some implementation on top of standard library types out of the box.
    """

    host: str = "localhost"
    port: int = 3306
    user: str = "root"
    password: str = ""
    database: str = ""
    charset: str = "utf8mb4"
    max_pool_size: int = 100
    max_spare_conns: int = 10
    min_spare_conns: int = 5
    max_conn_lifetime: int = 300
    max_conn_usage: int = 100
    connect_timeout: int = 1
    read_timeout: int = 60
    write_timeout: int = 60
    wait_timeout: Optional[int] = None
    remote_app: Optional[str] = None
    timezone: Optional[tzinfo] = timezone.utc
