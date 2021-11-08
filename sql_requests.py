# -*- coding: utf-8 -*-'


class CheckRequiredAttrsMCS(type):
    """Метакласс который алертит в случае если у его классов не назначен
    хоть один из аттрибутов указанный в required_attrs
    """
    required_attrs = 'create_table', 'table_name',

    def __new__(mcs, name, bases, attrs):
        if not all([
            param in attrs and isinstance(param, str) and bool(param)
            for param in mcs.required_attrs
        ]):
            raise NotImplementedError(
                f"У класса {name} обязательно "
                f"должны быть назначены атрибуты: {mcs.required_attrs}"
            )
        return super(CheckRequiredAttrsMCS, mcs).__new__(mcs, name, bases, attrs)


class TableQueriesCollection(metaclass=CheckRequiredAttrsMCS):
    """Класс коллекция запросов в БД связанных с конкретной таблицей
    """
    create_table: str = ' '
    table_name: str = ' '


class MattermostMessagesQuerys(TableQueriesCollection):
    """
    Запросы для таблицы mattermost_messages
    ---
    Таблица для хранения сообщений текст которых содержащих(опционально)
    определённую подстроку.
    """
    create_table = (
        "CREATE TABLE IF NOT EXISTS mattermost_messages( "
        "mattermost_message_id VARCHAR (200) PRIMARY KEY, "  # - id ообщения
        "filter_key VARCHAR NULL, "                          # - интересующая подстрака
        "channel_id VARCHAR (150) NOT NULL, "                # - канал из которого это сообщение
        "channel_name VARCHAR (250) NULL, "                  # - назв. этого канала
        "category VARCHAR (150) NULL, "                      # - категория сообщения (тема вопроса)
        "author_id VARCHAR (150) NOT NULL, "                 # - id отправителя
        "author VARCHAR (150) NULL, "                        # - Ник отправителя
        "timestamp DATETIME NULL, "                          # - ну ты понял
        "message_text TEXT"                                  # - текст сообщения
        ")"
    )
    table_name = 'mattermost_messages'
    get_all_messages = 'SELECT * FROM mattermost_messages'
    get_mattermost_message_by_jira_issue_key = 'select * from mattermost_messages where jira_issue_key=%s'
    create_new_message = """
    INSERT INTO 
        mattermost_messages (mattermost_message_id, filter_key, channel_id,
                             channel_name, author_id, author, timestamp, message_text) 
    VALUES 
        (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    delete_message_by_jira_issue_key = """delete from mattermost_messages where jira_issue_key=%s"""
    delete_all_messages = """DELETE FROM mattermost_messages"""

    create_multiple_messages_query = """
    INSERT INTO 
        mattermost_messages (mattermost_message_id, filter_key, channel_id, author_id, timestamp, message_text)
    VALUES 
        (%s, %s, %s, %s, %s, %s)
    """


