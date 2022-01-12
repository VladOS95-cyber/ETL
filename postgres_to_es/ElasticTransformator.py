import pydantic
from DataClasses import Film, Person, RecordGenres, RecordMovies, RecordPersons
from Utils import app_logger

logger = app_logger.get_logger(__name__)


def data_transform_for_movies(extracted_data):
    records = []
    for row in extracted_data:
        transformed_actors = _persons_and_films_getter(row.get("actors"), person=True)
        transformed_writers = _persons_and_films_getter(row.get("writers"), person=True)
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
        transformed_films = _persons_and_films_getter(row.get("films"), film=True)
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
        transformed_films = _persons_and_films_getter(row.get("films"), film=True)
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


def _persons_and_films_getter(data_list, person=False, film=False):
    record = []
    if data_list is None:
        return record
    for data in data_list:
        splitter = data.split(",")
        if person:
            record.append(Person(id=splitter[0], name=splitter[1]))
        if film:
            record.append(Film(id=splitter[0], title=splitter[1]))
    return record
