from dataclasses import dataclass


@dataclass
class MySQLConfig:
    """
    MySQL connection configuration.
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
