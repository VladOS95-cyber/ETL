import os
from datetime import datetime

from ETLCondition import JsonFileStorage, State
from Utils import app_logger

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STORAGE_FILE = "state_storage.json"
STORAGE_FILE_PATH = os.path.join(BASE_DIR, STORAGE_FILE)

logger = app_logger.get_logger(__name__)


logger.info("State reading")
state_storage = JsonFileStorage(STORAGE_FILE_PATH)
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
        query = f"""SELECT
        id, updated_at
        FROM content.film_work
        WHERE updated_at > '{film_work_date}';"""
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            state_control.set_state(
                "film_work", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            return [r.get("id") for r in record]
        else:
            cursor.close()


class PersonExtractor:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_fresh_persons(self, connection):
        query = f"""SELECT id, updated_at
        FROM content.person
        WHERE updated_at > '{person_date}'
        ORDER BY updated_at;"""
        cursor = connection.cursor()
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            return [r.get("id") for r in record]
        else:
            cursor.close()
            return []

    def extract_persons_in_films_participation(self, connection):
        fresh_persons = tuple(self.extract_fresh_persons(connection))
        if len(fresh_persons) == 0:
            return
        query = f"""SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.person_id IN {fresh_persons}
        ORDER BY fw.updated_at;"""
        cursor = connection.cursor()
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            return [r.get("id") for r in record]
        else:
            cursor.close()


class GenreExtractor:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_fresh_genres(self, connection):
        query = f"""SELECT id, updated_at
        FROM content.genre
        WHERE updated_at > '{genre_date}'
        ORDER BY updated_at;"""
        cursor = connection.cursor()
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            return [r.get("id") for r in record]
        else:
            cursor.close()
            return []

    def extract_genres_in_films(self, connection):
        fresh_genres = tuple(self.extract_fresh_genres(connection))
        if len(fresh_genres) == 0:
            return
        query = f"""SELECT fw.id, fw.updated_at
        FROM content.film_work fw
        LEFT JOIN content.genre_film_work pfw ON pfw.film_work_id = fw.id
        WHERE pfw.genre_id IN {fresh_genres}
        ORDER BY fw.updated_at;"""
        cursor = connection.cursor()
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            return [r.get("id") for r in record]
        else:
            cursor.close()


class PersonsDataEx:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_persons_data(self, connection, persons_id):
        if len(persons_id) == 0:
            return
        persons_data = []
        cursor = connection.cursor()
        query = f"""SELECT
            p.id as id,
            p.full_name as full_name,
            ARRAY_AGG(DISTINCT pfw.role) AS roles,
            ARRAY_AGG(DISTINCT CONCAT (pfw.film_work_id, ',', fw.title))
                FILTER ( WHERE pfw.film_work_id = fw.id ) AS films
            FROM content.person p
            LEFT JOIN content.person_film_work pfw ON pfw.person_id = p.id
            LEFT JOIN content.film_work fw ON fw.id = pfw.film_work_id
            WHERE p.id IN {tuple(persons_id)}
            GROUP BY p.id;"""
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            persons_data.append([rec for rec in record])
            state_control.set_state(
                "person", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            return persons_data
        else:
            cursor.close()


class GenresDataEx:
    def __init__(self, pg_connection):
        self.pg_connection = pg_connection

    def extract_genres_data(self, connection, genres_id):
        if len(genres_id) == 0:
            return
        genres_data = []
        cursor = connection.cursor()
        query = f"""SELECT
            g.id as id,
            g.name as name,
            g.description as description,
            ARRAY_AGG(DISTINCT CONCAT (gfw.film_work_id, ',', fw.title))
                FILTER ( WHERE gfw.film_work_id = fw.id ) AS films
            FROM content.genre g
            LEFT JOIN content.genre_film_work gfw ON gfw.genre_id = g.id
            LEFT JOIN content.film_work fw ON fw.id = gfw.film_work_id
            WHERE g.id IN {tuple(genres_id)}
            GROUP BY g.id;"""
        cursor.execute(query)
        record = cursor.fetchall()
        if record:
            genres_data.append([rec for rec in record])
            state_control.set_state(
                "genre", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            return genres_data
        else:
            cursor.close()


def extract_all_final_data(connection, film_ids):
    final_data = []
    if len(film_ids) == 0:
        return
    cursor = connection.cursor()
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
            WHERE fw.id IN {tuple(film_ids)}
            GROUP BY fw.id;"""
    cursor.execute(query)
    while record := cursor.fetchmany(100):
        final_data.append([rec for rec in record])
    cursor.close()
    return final_data


def extract_postgre_data_for_movies_index(connection):
    unique_changed_films_ids = set()

    film_work_extractor = FilmWorkExtractor(connection)
    film_work_data = film_work_extractor.extract_film_work_data(connection)
    if film_work_data is not None:
        for ids in film_work_data:
            unique_changed_films_ids.add(ids)

    genre_extractor = GenreExtractor(connection)
    genre_data = genre_extractor.extract_genres_in_films(connection)
    if genre_data is not None:
        for ids in genre_data:
            unique_changed_films_ids.add(ids)

    person_extractor = PersonExtractor(connection)
    person_data = person_extractor.extract_persons_in_films_participation(connection)
    if person_data is not None:
        for ids in person_data:
            unique_changed_films_ids.add(ids)

    final_data = extract_all_final_data(connection, unique_changed_films_ids)

    return final_data


def extract_postgres_data_for_persons_index(connection):
    person_extractor = PersonExtractor(connection)
    pers_ids = person_extractor.extract_fresh_persons(connection)
    person_ex = PersonsDataEx(connection)
    pers_data = person_ex.extract_persons_data(connection, pers_ids)
    if pers_data is not None:
        return pers_data


def extract_postgres_data_for_genress_index(connection):
    genre_extractor = GenreExtractor(connection)
    genr_ids = genre_extractor.extract_fresh_genres(connection)
    genre_ex = GenresDataEx(connection)
    genrs_data = genre_ex.extract_genres_data(connection, genr_ids)
    if genrs_data is not None:
        return genrs_data
