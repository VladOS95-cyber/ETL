import os

import psycopg2
import schedule
from elasticsearch import Elasticsearch, helpers
from psycopg2.extras import DictCursor

from Config import Config
from DataDownloader import extract_postgre_data
from ElasticTransformator import data_transform
from Utils import app_logger
from Utils.backoff import backoff


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config.json")
STORAGE_FILE = "state_storage.json"
CONFIG = Config.parse_file(CONFIG_FILE_PATH)
POSTGRE_DB_DSN = dict(CONFIG.film_work_pg.dsn)
ELASTIC_SETTINGS = dict(CONFIG.film_work_pg.elastic)

logger = app_logger.get_logger(__name__)


def load_data():
    es = Elasticsearch(
        [ELASTIC_SETTINGS["http"]],
        http_auth=(ELASTIC_SETTINGS["user_name"], ELASTIC_SETTINGS["password"]),
        port=ELASTIC_SETTINGS["port"],
    )
    postgre_data = download_postgre_data()
    if postgre_data is None:
        logger.info("No data for upload")
        return
    for data in postgre_data:
        upload_to_elastic(data, es)


@backoff()
def download_postgre_data():
    pg_conn = psycopg2.connect(**POSTGRE_DB_DSN, cursor_factory=DictCursor)
    return extract_postgre_data(pg_conn)


def upload_to_elastic(postgre_data, es):
    logger.info("Transform data for elastic uploading")
    records = data_transform(postgre_data)
    data = [
        {"_index": "movies", "_id": record.id, "_source": record.json()}
        for record in records
    ]
    logger.info("Uploading data into elastic index")
    helpers.bulk(es, data)


if __name__ == "__main__":
    load_data()
    logger.info("Start ETL service")
    schedule.every(1).minutes.do(load_data())

    while True:
        schedule.run_pending()
