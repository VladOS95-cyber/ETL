import time
from functools import wraps

import psycopg2
from Utils import app_logger

logger = app_logger.get_logger(__name__)


def backoff(start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while t < border_sleep_time:
                try:
                    logger.info("Trying connect to database")
                    return func(*args, **kwargs)
                except (psycopg2.DatabaseError, psycopg2.OperationalError) as error:
                    logger.error(error)
                    if t >= border_sleep_time:
                        raise error
                    logger.info(f"Wait for {t} seconds")
                    time.sleep(t)
                    t = t * factor

        return inner

    return func_wrapper
