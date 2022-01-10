import os

import psycopg2
from Config import Config
from DataDownloader import (extract_postgre_data_for_movies_index,
                            extract_postgres_data_for_genress_index,
                            extract_postgres_data_for_persons_index)
from elasticsearch import helpers
from ElasticTransformator import (data_transform_for_genres,
                                  data_transform_for_movies,
                                  data_transform_for_persons)
from psycopg2.extras import DictCursor
from Utils import app_logger
from Utils.backoff import backoff

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config.json")
CONFIG = Config.parse_file(CONFIG_FILE_PATH)
POSTGRE_DB_DSN = dict(CONFIG.film_work_pg.dsn)


logger = app_logger.get_logger(__name__)


def load_data_for_movies_index(es):
    postgre_data = download_postgre_data_for_movies_index()
    if postgre_data is None:
        logger.info("No data for upload")
        return
    for data in postgre_data:
        upload_to_movies_elastic(data, es)


def load_data_for_person_index(es):
    postgre_data = download_postgre_data_for_person_index()
    if postgre_data is None:
        logger.info("No data for upload")
        return
    for data in postgre_data:
        upload_to_persons_elastic(data, es)


def load_data_for_genre_index(es):
    postgre_data = download_postgre_data_for_genre_index()
    if postgre_data is None:
        logger.info("No data for upload")
        return
    for data in postgre_data:
        upload_to_genres_elastic(data, es)


@backoff()
def download_postgre_data_for_movies_index():
    pg_conn = psycopg2.connect(**POSTGRE_DB_DSN, cursor_factory=DictCursor)
    return extract_postgre_data_for_movies_index(pg_conn)


@backoff()
def download_postgre_data_for_person_index():
    pg_conn = psycopg2.connect(**POSTGRE_DB_DSN, cursor_factory=DictCursor)
    return extract_postgres_data_for_persons_index(pg_conn)


@backoff()
def download_postgre_data_for_genre_index():
    pg_conn = psycopg2.connect(**POSTGRE_DB_DSN, cursor_factory=DictCursor)
    return extract_postgres_data_for_genress_index(pg_conn)


def upload_to_movies_elastic(postgre_data, es):
    logger.info("Transform data for elastic uploading")
    records = data_transform_for_movies(postgre_data)
    data = [
        {"_index": "movies", "_id": record.id, "_source": record.json()}
        for record in records
    ]
    logger.info("Uploading data into elastic index")
    helpers.bulk(es, data)
    logger.info("Data sucessfully uploaded into elastic - movies")


def upload_to_persons_elastic(postgre_data, es):
    logger.info("Transform data for elastic uploading")
    records = data_transform_for_persons(postgre_data)
    data = [
        {"_index": "persons", "_id": record.id, "_source": record.json()}
        for record in records
    ]
    logger.info("Uploading data into elastic index")
    helpers.bulk(es, data)
    logger.info("Data sucessfully uploaded into elastic - persons")


def upload_to_genres_elastic(postgre_data, es):
    logger.info("Transform data for elastic uploading")
    records = data_transform_for_genres(postgre_data)
    data = [
        {"_index": "genres", "_id": record.id, "_source": record.json()}
        for record in records
    ]
    logger.info("Uploading data into elastic index")
    helpers.bulk(es, data)
    logger.info("Data sucessfully uploaded into elastic - genres")
