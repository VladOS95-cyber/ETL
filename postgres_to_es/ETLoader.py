import os
from datetime import datetime

import psycopg2
import schedule
from Config import Config
from DataDownloader import extract_postgre_data
from elasticsearch import Elasticsearch, helpers
from ElasticTransformator import data_transform
from ETLCondition import JsonFileStorage, State
from psycopg2.extras import DictCursor
from Utils import app_logger
from Utils.backoff import backoff

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config.json")
STORAGE_FILE = "state_storage.json"
CONFIG = Config.parse_file(CONFIG_FILE_PATH)
POSTGRE_DB_DSN = dict(CONFIG.film_work_pg.dsn)

logger = app_logger.get_logger(__name__)


state_storage = JsonFileStorage(STORAGE_FILE)
state_control = State(state_storage)

film_work_date = state_control.get_state("film_work")
person_date = state_control.get_state("person")
genre_date = state_control.get_state("genre")
logger.info("State successfully read")


def load_data():
    es = Elasticsearch(
        ["http://127.0.0.1"], http_auth=("elastic", "changeme"), port=9200
    )
    postgre_film_work_data = download_postgre_data("film_work")
    film_work_is_uploaded = upload_to_elastic(postgre_film_work_data, es)
    if film_work_is_uploaded is True:
        logger.info("Creation film_work_state")
        state_control.set_state("film_work", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    postgre_person_data = download_postgre_data("person")
    person_is_uploaded = upload_to_elastic(postgre_person_data, es)
    if person_is_uploaded is True:
        logger.info("Creation person_state")
        state_control.set_state("person", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    genre_film_work_data = download_postgre_data("genre")
    genre_is_uploaded = upload_to_elastic(genre_film_work_data, es)
    if genre_is_uploaded is True:
        logger.info("Creation genre_state")
        state_control.set_state("genre", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


@backoff()
def download_postgre_data(extractor):
    pg_conn = psycopg2.connect(**POSTGRE_DB_DSN, cursor_factory=DictCursor)
    yield extract_postgre_data(pg_conn, extractor)


def upload_to_elastic(postgre_data, es):
    for data in postgre_data:
        logger.info("Transform data for elastic uploading")
        records = data_transform(data)
        if records is None:
            logger.info("No fresh data in db")
            return False
        data = [
            {"_index": "movies", "_id": record.id, "_source": record.json()}
            for record in records
        ]
        logger.info("Uploading data into elastic index")
        helpers.bulk(es, data)
    return True

if __name__ == "__main__":
    logger.info("Start ETL service")
    schedule.every(1).minutes.do(load_data())

    while True:
        schedule.run_pending()
