from __future__ import annotations

from typing import Any

import aiomysql
from pymysql.constants import COMMAND
from pymysql.converters import decoders

from .._sl import _SL
from .cursor import Cursor
from .observer import QueryObserver, LoggingObserver, _QueryObserverContext, ObserverContext
from .. import logger


class AioMySQLConnection(aiomysql.Connection):
    def __init__(self, host="localhost", user=None, password="",
                 db=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, read_default_group=None,
                 autocommit=False, echo=False,
                 local_infile=False, loop=None, ssl=None, auth_plugin='',
                 program_name='', server_public_key=None):
        super().__init__(host, user, password, db, port, unix_socket, charset, sql_mode,
                         read_default_file, conv, use_unicode, client_flag, cursorclass, init_command,
                         connect_timeout, read_default_group, autocommit, echo, local_infile, loop, ssl, auth_plugin,
                         program_name, server_public_key)

        self.logger = logger.getChild(f"mysql.{self.db}") if self.db else logger.getChild("mysql")
        self.observers: set[QueryObserver] = {
            LoggingObserver(self.logger)
        }

    def attach(self, observer: QueryObserver):
        self.observers.add(observer)

    def detach(self, observer: QueryObserver):
        self.observers.remove(observer)

    def observe_query_before(self, sql: str, **kwargs: Any) -> dict[int, Any]:
        observer_context: dict[int, Any] = {}

        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer_context[id(observer)] = observer.observe_query_before(
                        self,
                        sql,
                        **ctx.kwargs()
                    )

            return observer_context

    def observe_query_after(self, sql: str, observer_context: dict[int, Any], **kwargs: Any):
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_after(
                        self,
                        sql,
                        observer_context.get(id(observer)),
                        **ctx.kwargs()
                    )

    def observe_query_error(self, sql: str, exc: Exception, observer_context: dict[int, Any], **kwargs: Any):
        with _SL():
            with ObserverContext(**kwargs) as ctx:
                for observer in self.observers:
                    observer.observe_query_error(
                        self,
                        sql,
                        exc,
                        observer_context.get(id(observer)),
                        **ctx.kwargs()
                    )

    async def _execute_command(self, command, sql: str | bytes):
        with _SL():
            if command == COMMAND.COM_QUERY:
                ctx = _QueryObserverContext.query_ctx.get()
                if ctx is not None:
                    ctx.sql = sql.decode("utf-8") if isinstance(sql, bytes) else sql
                    ctx.observer_context = self.observe_query_before(ctx.sql, **ctx.kwargs())

            return await super()._execute_command(command, sql)

    async def query(self, sql: str, unbuffered=False, **kwargs: Any):
        with _SL(4):
            with _QueryObserverContext(self, **kwargs):
                return await super().query(sql, unbuffered)

    async def _send_autocommit_mode(self, **kwargs: Any):
        with _SL(4):
            with _QueryObserverContext(self, **kwargs):
                return await super()._send_autocommit_mode()

    async def begin(self, **kwargs: Any):
        with _SL():
            with _QueryObserverContext(self, **kwargs):
                return await super().begin()

    async def commit(self, **kwargs: Any):
        with _SL(3):
            with _QueryObserverContext(self, **kwargs):
                return await super().commit()

    async def rollback(self, **kwargs: Any):
        with _SL(3):
            with _QueryObserverContext(self, **kwargs):
                return await super().rollback()

    async def show_warnings(self, **kwargs: Any):
        with _QueryObserverContext(self, **kwargs):
            await super().show_warnings()

    async def set_charset(self, charset, **kwargs: Any):
        with _QueryObserverContext(self, **kwargs):
            await super().set_charset(charset)

    def __repr__(self):
        return f"<aiomysql.Connection 0x{id(self):x} {self._user}@{self._host}:{self._port}/{self._db}>"
