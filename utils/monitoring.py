""" Monitoring module """

from functools import wraps
from time import time


class Monitoring:
    """ The main monitoring class """
    @staticmethod
    def timing(func):
        """ Timing show """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time()
            result = func(*args, **kwargs)
            end = time()
            time_elapsed = round(end - start, 4)

            if result["log_txt"]:
                print("{}. Elapsed time: {} sec".format(result["log_txt"], time_elapsed))
            else:
                print("func: {}. Elapsed time: {} sec".format(func.__name__, time_elapsed))
            return result
        return wrapper
