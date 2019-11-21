""" Monitoring module """

import logging
import sys
from functools import wraps
from time import time


class Monitoring:
    """ The main monitoring class """
    def __init__(self, name):
        self.__logger = self.logger_setup(name)
        self.start_time = None
        self.end_time = None

    @staticmethod
    def logger_setup(name):
        """ Return a new logger """
        logger = logging.getLogger(name)

        if not logger.handlers:
            logger.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)20s - %(levelname)s - %(message)s', '%Y-%m-%d %H:%M:%S')
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        return logger

    def info(self, message):
        """ Create a new log message into stdout """
        self.__logger.info(message)

    def error(self, message):
        """ Create a new log message into stdout """
        self.__logger.error(message)

    def wrapper(self, pre, post):
        """ Wrapper """
        def decorate(func):
            """ Decorator """
            @wraps(func)
            def call(*args, **kwargs):
                """ Actual wrapping """
                pre(func)
                result = func(*args, **kwargs)
                post(func)
                return result
            return call
        return decorate

    def entering(self, func):
        """ Pre function logging """
        self.__logger.info('Entered %s function', func.__name__)

    def exiting(self, func):
        """ Post function logging """
        self.__logger.info('Exited %s function', func.__name__)

    def time_start(self, func):
        self.start_time = time()

    def get_time_elapsed(self, func):
        self.end_time = time()
        time_elapsed = round(self.end_time - self.start_time, 4)
        # self.__logger.info('Elapsed time: {} sec'.format(time_elapsed))
        print('Elapsed time: {} sec'.format(time_elapsed))

    @staticmethod
    def timing(func):
        """ Timing show """
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time()
            result = func(*args, **kwargs)
            end = time()
            time_elapsed = round(end - start, 4)

            if result:
                print('{}. Elapsed time: {} sec'.format(result.get('log_txt', ''), time_elapsed))
            else:
                print('func: {}. Elapsed time: {} sec'.format(func.__name__, time_elapsed))
            return result
        return wrapper
