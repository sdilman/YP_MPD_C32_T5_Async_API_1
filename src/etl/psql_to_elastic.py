import logging.config
import time
from contextlib import closing
from dataclasses import asdict

import backoff
import psycopg2
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ElasticConnectionError
from elasticsearch.helpers import bulk
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from dataclasses_storage import FilmWork
from elastic_schema import schema
from sql_bank import *
from state import JsonFileStorage, State
from config import settings

load_dotenv()
logging.config.fileConfig('logger.conf')


class PostgresExtractor:
    def __init__(self, pg_conn: _connection):
        self.connect = pg_conn

    def extract_modified_data(self,
                              timer_genres: str,
                              timer_persons: str,
                              timer_filmworks: str):
        """Выгрузка измененных данных из PostgreSQL"""

        def extract_timer_and_ids(data: list, timer: str):
            if data:
                last_input = data[-1][1].isoformat()
                id = tuple(i[0] for i in data)
                if len(data) == 1:
                    id = tuple([id[0]])
            else:
                last_input = timer
                id = tuple(["00000000-0000-0000-0000-000000000000"])
            return last_input, id

        cursor = self.connect.cursor()
        cursor.execute(SQL_LAST_INSERTED_PERSONS, (timer_persons, settings.PERSON_LIMIT))
        data_persons = cursor.fetchall()
        last_input_persons, persons_id = extract_timer_and_ids(data_persons, timer_persons)

        cursor.execute(SQL_LAST_INSERTED_GENRES, (timer_genres, settings.GENRE_LIMIT))
        data_genres = cursor.fetchall()
        last_input_genres, genres_id = extract_timer_and_ids(data_genres, timer_genres)

        cursor.execute(SQL_LAST_INSERTED_TIME_FILMWORKS, (timer_filmworks, settings.FILMWORK_LIMIT))
        data_filmworks = cursor.fetchall()
        last_input_filmworks, filmworks_id = extract_timer_and_ids(data_filmworks, timer_filmworks)

        cursor.execute(ALL_MODIFIED_MOVIES, (persons_id, genres_id, filmworks_id))
        data_from_postgres = cursor.fetchall()
        return data_from_postgres, [last_input_genres, last_input_persons, last_input_filmworks]


class DataTransform:
    def data_from_postgres_to_elastic_format(self, rows: list):
        """Подготовка данных для вставки в ElasticSearch"""

        def removekey(d: dict, key: str):
            del d[key]
            return d

        data = [removekey(asdict(FilmWork(*movie)), "persons") for movie in rows]
        return data


class ElasticsearchLoader:
    def upload_to_elastic(self, all_movies_elastic: list):
        """Вставка данных в ElasticSearch"""
        with Elasticsearch(f'http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}') as es:
            actions = [
                {
                    "_index": "movies",
                    "_id": movie["id"],
                    "_source": movie,
                    "doc_as_upsert": True
                } for movie in all_movies_elastic
            ]
            bulk(es, actions)
            logging.info(f"В ElasticSearch обновлены {len(all_movies_elastic)} фильмов")


def postgres_to_elastic(pg_conn: _connection):
    """Основной метод загрузки данных из Postgres в Elastic"""
    while True:
        timer_genres = state.get_state("timer_genres")
        timer_persons = state.get_state("timer_persons")
        timer_filmworks = state.get_state("timer_filmworks")
        postgres_exporter = PostgresExtractor(pg_conn)
        data_from_postgres, timers = postgres_exporter.extract_modified_data(timer_genres,
                                                                             timer_persons,
                                                                             timer_filmworks)
        transfer = DataTransform()
        all_movies_elastic = transfer.data_from_postgres_to_elastic_format(data_from_postgres)
        if all_movies_elastic:
            loader = ElasticsearchLoader()
            loader.upload_to_elastic(all_movies_elastic)
            state.set_state("timer_genres", timers[0])
            state.set_state("timer_persons", timers[1])
            state.set_state("timer_filmworks", timers[2])
        else:
            logging.info("Нет данных для обновления")
        time.sleep(settings.RELAX_TIME)


def backoff_hdlr(details):
    logging.info(f"backoff_hdlr, {details}")


@backoff.on_exception(backoff.expo, (
                          psycopg2.OperationalError,
                          psycopg2.InterfaceError,
                          ElasticConnectionError
                          ),
                      max_value=10,
                      on_backoff=backoff_hdlr)
def main():
    with closing(psycopg2.connect(**settings.dsl, cursor_factory=DictCursor)) as pg_conn:
        postgres_to_elastic(pg_conn)


if __name__ == "__main__":
    storage = JsonFileStorage("state.json")
    state = State(storage)
    state.set_state("timer_genres", settings.TIMER_GENRES)
    state.set_state("timer_persons", settings.TIMER_PERSONS)
    state.set_state("timer_filmworks", settings.TIMER_FILMWORKS)
    es = Elasticsearch(f'http://{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}')
    try:
        es.indices.create(index="movies", body=schema)
        logging.info('Индекс movies создан')
    except:
        logging.info('Индекс movies уже существует')
    es.transport.close()

    main()
