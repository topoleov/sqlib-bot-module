from typing import Collection, Dict, List, Iterable
from core.sqllib.sql_requests import TableQueriesCollection, MattermostMessagesQuerys
import psycopg2.errors

import inspect
from logging import Logger
from functools import wraps
from time import sleep
from logger import logger

import psycopg2
from psycopg2.extras import DictCursor, NamedTupleCursor


class SQLConnectorBase:
    """
    Базовый SQLConnector класс для дальнейшей работы с SQL (через коннектор)
    Может испольщоваться как контекстный менеджер.
    """
    # Список таблиц для которых класс предоставляет методы работы
    tables: Collection[TableQueriesCollection]

    def __init__(
            self,
            db_name: str = None,
            db_host: str = None,
            db_port: int = None,
            db_username: str = None,
            db_password: str = None,
    ):
        self.db_name = db_name
        self.db_host = db_host
        self.db_port = db_port
        self.db_username = db_username
        self.db_password = db_password

        self.connection = None
        self.cursor = None

    def __enter__(self):
        self.connection = psycopg2.connect(
            dbname=self.db_name,
            host=self.db_host,
            port=self.db_port,
            user=self.db_username,
            password=self.db_password
        )
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

    def create_tables(self, tables: Collection[TableQueriesCollection] = None) -> None:
        """
        Создание таблиц
        ---
        :param tables: список таблиц которые надо создать
        :raises:
            psycopg2.errors.SyntaxError: Ошибка синтаксиса SQL
            psycopg2.OperationalError: отсутсвует подключение к базе
            ValueError: В tables переданы не корректные данные
        """
        with self.db_session as db:
            for queries_collection in tables or self.tables:
                if not issubclass(queries_collection, TableQueriesCollection):
                    raise ValueError(
                        "В `tables` должна быть переданна коллекция типа TableQueriesCollection"
                    )
                try:
                    # create_table по любому есть
                    db.cursor.execute(queries_collection.create_table)
                    db.connection.commit()
                # Перехватываем SyntaxError
                except psycopg2.errors.lookup('42601') as e:
                    # Откатываем уже созданные таблицы
                    db.connection.rollback()
                    raise e
                # Перехватываем DuplicateTable
                except psycopg2.errors.lookup('42P07'):
                    continue

    @property
    def db_session(self):
        return self

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()
