import os

import psycopg2
import schedule
from Config import Config
from elasticsearch import Elasticsearch
from ElasticUploader import (load_data_for_genre_index,
                             load_data_for_movies_index,
                             load_data_for_person_index)
from Utils import app_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config.json")
CONFIG = Config.parse_file(CONFIG_FILE_PATH)
POSTGRE_DB_DSN = dict(CONFIG.film_work_pg.dsn)
ELASTIC_SETTINGS = dict(CONFIG.film_work_pg.elastic)

logger = app_logger.get_logger(__name__)


es = Elasticsearch(hosts=[f'{ELASTIC_SETTINGS["host"]}:{ELASTIC_SETTINGS["port"]}'])


def load_data():
    load_data_for_movies_index(es)
    load_data_for_person_index(es)
    load_data_for_genre_index(es)


if __name__ == "__main__":
    logger.info("Start ETL service")
    schedule.every(1).seconds.do(load_data)

    while True:
        schedule.run_pending()
