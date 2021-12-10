import os

from ETLCondition import JsonFileStorage, State
from Utils import app_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORAGE_FILE = "state_storage.json"


logger = app_logger.get_logger(__name__)


logger.info("State reading")
state_storage = JsonFileStorage(STORAGE_FILE)
state_control = State(state_storage)
film_work_date = state_control.get_state("film_work")
person_date = state_control.get_state("person")
genre_date = state_control.get_state("genre")
logger.info("State successfully read")


class FilmWorkExtractor:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_film_work_data(self, connection):
        cursor = connection.cursor()
        offset = 0
        limit = 100
        query = f"""SELECT
        fw.id as id,
        fw.rating as imdb_rating,
        ARRAY_AGG(DISTINCT g.name ) AS genre,
        fw.title as title,
        fw.description as description,
        ARRAY_AGG(DISTINCT p.full_name )
            FILTER ( WHERE pfw.role = 'director' ) AS director,
        ARRAY_AGG(DISTINCT p.full_name)
            FILTER ( WHERE pfw.role = 'actor' ) AS actors_names,
        ARRAY_AGG(DISTINCT p.full_name)
            FILTER ( WHERE pfw.role = 'writer' ) AS writers_names,
        ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
            FILTER ( WHERE pfw.role = 'actor' ) AS actors,
        ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
            FILTER ( WHERE pfw.role = 'writer' ) AS writers
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        LEFT JOIN content.person p ON p.id = pfw.person_id
        LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
        LEFT JOIN content.genre g ON g.id = gfw.genre_id
        WHERE fw.updated_at > '{film_work_date}'
        GROUP BY fw.id
        LIMIT {limit}
        OFFSET {offset};"""
        while True:
            cursor.execute(query)
            record = cursor.fetchall()
            if record:
                yield record
                offset += limit
            else:
                cursor.close()
                break


class PersonExtractor:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_fresh_persons(self, connection):
        limit = 100
        offset = 0
        query = f"""SELECT id, updated_at
        FROM content.person
        WHERE updated_at > '{person_date}'
        ORDER BY updated_at
        LIMIT {limit}
        OFFSET {offset};"""
        while True:
            cursor = connection.cursor()
            cursor.execute(query)
            record = cursor.fetchall()
            if record:
                yield record
                offset += limit
            else:
                break

    def extract_persons_in_films_participation(self, connection, person_ids):
        limit = 100
        offset = 0
        query = f"""SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN ({person_ids})
        ORDER BY fw.updated_at
        LIMIT {limit}
        OFFSET {offset};"""
        while True:
            cursor = connection.cursor()
            cursor.execute(query)
            record = cursor.fetchall()
            if record:
                yield record
                offset += limit
            else:
                break

    def extract_final_person_data(self, connection):
        cursor = connection.cursor()
        offset = 0
        limit = 100
        for person_ids in self.extract_fresh_persons(connection):
            ids_person = ", ".join(f"'{ids_p.get('id')}'" for ids_p in person_ids)
            for person_film in self.extract_persons_in_films_participation(
                connection, ids_person
            ):
                ids = ", ".join(f"'{ids_fwp.get('id')}'" for ids_fwp in person_film)
                query = f"""SELECT
                fw.id as id,
                fw.rating as imdb_rating,
                ARRAY_AGG(DISTINCT g.name ) AS genre,
                fw.title as title,
                fw.description as description,
                ARRAY_AGG(DISTINCT p.full_name )
                    FILTER ( WHERE pfw.role = 'director' ) AS director,
                ARRAY_AGG(DISTINCT p.full_name)
                    FILTER ( WHERE pfw.role = 'actor' ) AS actors_names,
                ARRAY_AGG(DISTINCT p.full_name)
                    FILTER ( WHERE pfw.role = 'writer' ) AS writers_names,
                ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
                    FILTER ( WHERE pfw.role = 'actor' ) AS actors,
                ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
                    FILTER ( WHERE pfw.role = 'writer' ) AS writers
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.id IN ({ids})
                GROUP BY fw.id
                LIMIT {limit}
                OFFSET {offset};"""
                while True:
                    cursor.execute(query)
                    record = cursor.fetchall()
                    if record:
                        yield record
                        offset += limit
                    else:
                        cursor.close()
                        break


class GenreExtractor:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_fresh_genres(self, connection):
        limit = 100
        offset = 0
        query = f"""SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > '{genre_date}'
        ORDER BY updated_at
        LIMIT {limit}
        OFFSET {offset};"""
        while True:
            cursor = connection.cursor()
            cursor.execute(query)
            record = cursor.fetchall()
            if record:
                yield record
                offset += limit
            else:
                break

    def extract_genres_in_films(self, connection, person_ids):
        limit = 100
        offset = 0
        query = f"""SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.genre_id IN ({person_ids})
        ORDER BY fw.updated_at
        LIMIT {limit}
        OFFSET {offset};"""
        while True:
            cursor = connection.cursor()
            cursor.execute(query)
            record = cursor.fetchall()
            if record:
                yield record
                offset += limit
            else:
                break

    def extract_final_genre_data(self, connection):
        cursor = connection.cursor()
        offset = 0
        limit = 100
        for genre_ids in self.extract_fresh_genres(connection):
            ids_genre = ", ".join(f"'{ids_g.get('id')}'" for ids_g in genre_ids)
            for genre_film in self.extract_genres_in_films(connection, ids_genre):
                ids = ", ".join(f"'{ids_fwg.get('id')}'" for ids_fwg in genre_film)
                query = f"""SELECT
                fw.id as id,
                fw.rating as imdb_rating,
                ARRAY_AGG(DISTINCT g.name ) AS genre,
                fw.title as title,
                fw.description as description,
                ARRAY_AGG(DISTINCT p.full_name )
                    FILTER ( WHERE pfw.role = 'director' ) AS director,
                ARRAY_AGG(DISTINCT p.full_name)
                    FILTER ( WHERE pfw.role = 'actor' ) AS actors_names,
                ARRAY_AGG(DISTINCT p.full_name)
                    FILTER ( WHERE pfw.role = 'writer' ) AS writers_names,
                ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
                    FILTER ( WHERE pfw.role = 'actor' ) AS actors,
                ARRAY_AGG(DISTINCT CONCAT (p.id, ',', p.full_name))
                    FILTER ( WHERE pfw.role = 'writer' ) AS writers
                FROM content.film_work fw
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id
                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                WHERE fw.id IN ({ids})
                GROUP BY fw.id
                LIMIT {limit}
                OFFSET {offset};"""
                while True:
                    cursor.execute(query)
                    record = cursor.fetchall()
                    if record:
                        yield record
                        offset += limit
                    else:
                        cursor.close()
                        break


def extract_postgre_data(connection, extractor_name):
    if extractor_name == "film_work":
        logger.info("Start extraction film_work data")
        film_work_extractor = FilmWorkExtractor(connection)
        for data in film_work_extractor.extract_film_work_data(connection):
            yield data
    elif extractor_name == "genre":
        logger.info("Start extraction genre data")
        genre_extractor = GenreExtractor(connection)
        for data in genre_extractor.extract_final_genre_data(connection):
            yield data
    else:
        logger.info("Start extraction person data")
        person_extractor = PersonExtractor(connection)
        for data in person_extractor.extract_final_person_data(connection):
            yield data
