from typing import List

from core.sqllib.sql_requests import MattermostMessagesQuerys
from core.sqllib.sqlbase import SQLConnectorBase
from api.mattermost.v4.models import Post


class SQLMattermost(SQLConnectorBase):

    tables = [MattermostMessagesQuerys]

    def save_messages(self, posts: List[Post]):
        """
        Сохраняем сообщения
        ---
        :raises:
            ValueError: В posts переданы не корректные данные
            UniqueViolation: сообщение с таким `id` уже сохранено
            psycopg2.errors.SyntaxError: Ошибка синтаксиса SQL
            psycopg2.OperationalError: отсутсвует подключение к базе
        """
        if not all([isinstance(post, Post) for post in posts]):
            raise ValueError(
                "В `tables` должна быть переданна коллекция типа TableQueriesCollection"
            )
        rows = [
            (post.id, post.filter_key, post.channel_id, post.user_id, post.create_at, post.message)
            for post in posts
        ]
        with self.db_session as db:
            db.cursor.executemany(
                MattermostMessagesQuerys.create_multiple_messages_query,
                rows
            )
            db.connection.commit()

    def save_message(self, post: Post):
        """
        Сохраняем сообщение
        ---
        :raises:
            ValueError: В post переданы не корректные данные
            UniqueViolation: сообщение с таким `id` уже сохранено
            psycopg2.errors.SyntaxError: Ошибка синтаксиса SQL
            psycopg2.OperationalError: отсутсвует подключение к базе
        """
        if not isinstance(post, Post):
            raise ValueError("Тип аргумент `post` должен быть инстансом класса Post")
        row = (
            post.id, post.filter_key, post.channel_id, post.channel_display_name,
            post.user_id, post.sender_name, post.create_at, post.message,
        )
        with self.db_session as db:
            db.cursor.execute(
                MattermostMessagesQuerys.create_new_message,
                row
            )
            db.connection.commit()

    def delete_all_messages(self):
        """
        Удалить все сообщение
        ---
        :raises:
            psycopg2.OperationalError: отсутсвует подключение к базе
        """
        with self.db_session as db:
            db.cursor.execute(
                MattermostMessagesQuerys.delete_all_messages
            )
            db.connection.commit()
