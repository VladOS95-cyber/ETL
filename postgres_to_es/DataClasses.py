from typing import List, Union

from pydantic import BaseModel


class Person(BaseModel):
    id: str
    name: str


class Film(BaseModel):
    id: str
    title: str


class RecordMovies(BaseModel):
    id: str
    imdb_rating: Union[float, None] = 0.0
    genre: List[str] = []
    title: str
    description: Union[str, None] = ""
    director: Union[str, None] = ""
    actors_names: List[str] = []
    writers_names: List[str] = []
    actors: List[Person] = []
    writers: List[Person] = []


class RecordPersons(BaseModel):
    id: str
    full_name: str
    roles: List[str] = []
    films: List[Film] = []


class RecordGenres(BaseModel):
    id: str
    name: str
    description: Union[str, None] = ""
    films: List[Film] = []
