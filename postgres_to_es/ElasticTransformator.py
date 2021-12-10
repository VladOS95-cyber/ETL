import pydantic
from DataClasses import Person, Record
from Utils import app_logger

logger = app_logger.get_logger(__name__)


def data_transform(extracted_data):
    records = []
    for data in extracted_data:
        for row in data:
            transformed_actors = _persons_getter(row.get("actors"))
            transformed_writers = _persons_getter(row.get("writers"))
            try:
                records.append(
                    Record(
                        id=row.get("id"),
                        genre=row.get("genre"),
                        imdb_rating=row.get("rating"),
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


def _persons_getter(persons_list):
    record = []
    if persons_list is None:
        return record
    for person in persons_list:
        id_and_name = person.split(",")
        record.append(Person(id=id_and_name[0], name=id_and_name[1]))
    return record
