import pydantic
from DataClasses import Film, Person, RecordGenres, RecordMovies, RecordPersons
from Utils import app_logger

logger = app_logger.get_logger(__name__)


def data_transform_for_movies(extracted_data):
    records = []
    for row in extracted_data:
        transformed_actors = _persons_getter(row.get("actors"))
        transformed_writers = _persons_getter(row.get("writers"))
        try:
            records.append(
                RecordMovies(
                    id=row.get("id"),
                    genre=row.get("genre"),
                    imdb_rating=row.get("imdb_rating"),
                    title=row.get("title"),
                    description=row.get("description"),
                    director=row.get("director").pop()
                    if row.get("director") is not None
                    else None,
                    actors_names=row.get("actors_names")
                    if row.get("actors_names") is not None
                    else [],
                    writers_names=row.get("writers_names")
                    if row.get("writers_names") is not None
                    else [],
                    actors=transformed_actors,
                    writers=transformed_writers,
                )
            )

        except pydantic.error_wrappers.ValidationError as err:
            logger.error(err)

    return records


def data_transform_for_persons(extracted_data):
    records = []
    for row in extracted_data:
        transformed_films = _films_getter(row.get("films"))
        try:
            records.append(
                RecordPersons(
                    id=row.get("id"),
                    full_name=row.get("full_name"),
                    roles=row.get("roles"),
                    films=transformed_films,
                )
            )
        except pydantic.error_wrappers.ValidationError as err:
            logger.error(err)

    return records


def data_transform_for_genres(extracted_data):
    records = []
    for row in extracted_data:
        transformed_films = _films_getter(row.get("films"))
        try:
            records.append(
                RecordGenres(
                    id=row.get("id"),
                    name=row.get("name"),
                    description=row.get("description"),
                    films=transformed_films,
                )
            )
        except pydantic.error_wrappers.ValidationError as err:
            logger.error(err)

    return records


def _films_getter(films_list):
    record = []
    if films_list is None:
        return record
    for film in films_list:
        id_and_title = film.split(",")
        record.append(Film(id=id_and_title[0], title=id_and_title[1]))
    return record


def _persons_getter(persons_list):
    record = []
    if persons_list is None:
        return record
    for person in persons_list:
        id_and_name = person.split(",")
        record.append(Person(id=id_and_name[0], name=id_and_name[1]))
    return record
