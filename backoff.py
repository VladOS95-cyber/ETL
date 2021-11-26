from functools import wraps
import time

import psycopg2


def backoff(exception, start_sleep_time=0.1, factor=2, border_sleep_time=10):
    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            t = start_sleep_time
            while t < border_sleep_time:
                try:
                    return func(*args, **kwargs)
                except (
                    psycopg2.DatabaseError,
                    psycopg2.OperationalError
                ) as error:
                    if t >= border_sleep_time:
                        raise error
                    time.sleep(t)
                    t = t * factor
        return inner
    return func_wrapper
