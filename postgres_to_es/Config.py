from typing import List, Optional

from pydantic import BaseModel


class DSNSettings(BaseModel):
    host: str
    port: int
    dbname: str
    password: str
    user: str
    options: str


class ElasticSettings(BaseModel):
    host: str
    user_name: str
    password: str
    port: int


class PostgresSettings(BaseModel):
    elastic: ElasticSettings
    dsn: DSNSettings
    limit: Optional[int]
    order_field: List[str]
    state_field: List[str]
    fetch_delay: Optional[float]
    state_file_path: Optional[str]
    sql_query: str


class Config(BaseModel):
    film_work_pg: PostgresSettings
