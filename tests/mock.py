import datetime
from dataclasses import dataclass, field
from typing import Any, Optional, ClassVar

import aiomysql
import pytest
from pymysql.constants import FIELD_TYPE


@dataclass
class MockResult:
    insert_id: int = 0
    affected_rows: int = 0
    rows: list[dict[str, Any]] = field(default_factory=list)
    description: Optional[list[tuple[str, int, Any, int, int, int, int]]] = None

    def _type_from_value(self, value: Any) -> int:
        if isinstance(value, str):
            return FIELD_TYPE.STRING
        elif isinstance(value, int):
            return FIELD_TYPE.LONG
        elif isinstance(value, float):
            return FIELD_TYPE.DOUBLE
        elif isinstance(value, datetime.datetime):
            return FIELD_TYPE.DATETIME
        elif isinstance(value, datetime.date):
            return FIELD_TYPE.DATE
        elif isinstance(value, datetime.time):
            return FIELD_TYPE.TIME
        elif isinstance(value, bool):
            return FIELD_TYPE.BIT
        else:
            return FIELD_TYPE.BLOB

    def __post_init__(self):
        if self.rows:
            self.description = [(key, self._type_from_value(self.rows[0][key]), None, 0, 0, 0, 0) for key in self.rows[0].keys()]


class MockConnectionWrapper(aiomysql.Connection):
    QUERIES: ClassVar[dict[str, MockResult | Exception]] = {}

    @classmethod
    def reset(cls):
        cls.QUERIES = {
            "BEGIN": MockResult(),
            "COMMIT": MockResult(),
            "ROLLBACK": MockResult(),
            "SELECT 1": MockResult(rows=[{"1": 1}]),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._last_executed_query: Optional[str] = None
        self.QUERIES_EXECUTED = list()
        self.reset()

    def _ensure_alive(self):
        pass

    async def _execute_command(self, command, sql):
        if isinstance(sql, bytes):
            sql = sql.decode("utf-8")

        if sql:
            if sql in self.__class__.QUERIES:
                print("Matched sql against queries.")
                result = self.__class__.QUERIES[sql]

                if isinstance(result, Exception):
                    raise result

                self._result = MockMySQLResult(self, result)
            else:
                self._result = MockMySQLResult(self, MockResult())

            self.QUERIES_EXECUTED.append(sql)

    async def _read_ok_packet(self):
        return True

    async def _connect(self):
        pass

    async def _read_query_result(self, unbuffered=False):
        pass


class MockMySQLResult(aiomysql.connection.MySQLResult):
    def __init__(self, connection: MockConnectionWrapper, mock: MockResult):
        super().__init__(connection)
        self.mock = mock
        self.affected_rows = len(mock.rows) or mock.affected_rows
        self.insert_id = mock.insert_id
        self.field_count = 0 if not mock.rows else len(mock.rows[0])

        if mock.rows:
            first_row = mock.rows[0]
            self.description = tuple((key, self._type_from_value(first_row[key]), None, 0, 0, 0, 0) for key in first_row.keys())
            self.rows = tuple(tuple(row.values()) for row in mock.rows)
        else:
            self.description = None
            self.rows = None

    def _type_from_value(self, value):
        if isinstance(value, str):
            return FIELD_TYPE.STRING
        elif isinstance(value, int):
            return FIELD_TYPE.LONG
        elif isinstance(value, float):
            return FIELD_TYPE.DOUBLE
        elif isinstance(value, datetime.datetime):
            return FIELD_TYPE.DATETIME
        elif isinstance(value, datetime.date):
            return FIELD_TYPE.DATE
        elif isinstance(value, datetime.time):
            return FIELD_TYPE.TIME
        elif isinstance(value, bool):
            return FIELD_TYPE.BIT
        else:
            return FIELD_TYPE.BLOB


aiomysql.Connection = MockConnectionWrapper
aiomysql.connection.Connection = MockConnectionWrapper
aiomysql.connection.MySQLResult = MockMySQLResult


@pytest.fixture(scope="function", autouse=True)
def reset_mock():
    MockConnectionWrapper.reset()
