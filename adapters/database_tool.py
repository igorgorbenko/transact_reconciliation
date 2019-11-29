#!/usr/bin/env python3

""" Base class for the database """

import threading
from multiprocessing import Process
from multiprocessing import Queue
import configparser

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.pool import ThreadedConnectionPool

from utils.monitoring import Monitoring


config = configparser.ConfigParser()
config.read('./conf/db.ini')

DB_URL = config.get('POSTGRESQL', 'db_url')

m = Monitoring('db_tool')


class PostgreSQLMultiThread:
    """ Multi-threaded database work """
    _select_conn_count = 10
    _select_conn_pool = None

    data_queque = Queue()  # reader reads data from queue

    def __init__(self, str_sql, total_records):
        self.str_sql = str_sql
        self.total_records = total_records

        # calculate the max and min connection required
        self.pid_max = self.total_records // 100000

    def create_connection_pool(self):
        """ Create the thread safe threaded postgres connection pool"""

        max_conn = self.pid_max#self._select_conn_count
        min_conn = max_conn / 2

        # creating separate connection for read and write purpose
        self._select_conn_pool = ThreadedConnectionPool(min_conn,
                                                        max_conn,
                                                        DB_URL)

    @staticmethod
    def chunks(array, start, num):
        """Yield successive n-sized chunks from array"""
        for i in range(start, len(array), num):
            yield array[i:i + num]

    @classmethod
    def get_threads(cls, start=0,
                    num=1000, div=10):
        """ Split input value into equal chunks """
        inter = (num - start) // div
        mod = num % div
        threads_arr = []

        gener_list = list(cls.chunks(range(0, num), start, inter + mod))

        for gen in gener_list:
            threads_arr.append([gen.start, gen.stop])

        return threads_arr

    # @m.timing
    def read_data(self):
        """
        Read the data from the postgres and shared those records with each
        processor to perform their operation using threads
        Here we calculate the pardition value to help threading to read data from database
        """
        # pid_max = self.total_records // 100000

        threads_array = self.get_threads(0,
                                         self.total_records,
                                         self.pid_max)

        for pid in range(1, self.pid_max):
            # Getting connection from the connection pool
            select_conn = self._select_conn_pool.getconn()
            select_conn.autocommit = 1

            #Creating 10 process to perform the operation
            process = Process(target=self.process_data,
                              args=(self.data_queque,
                                    pid,
                                    threads_array[pid-1][0],
                                    threads_array[pid-1][1],
                                    select_conn))

            process.daemon = True
            process.start()
            process.join()
            select_conn.close()

            m.info('Process %s' % pid)


    # @m.timing
    def process_data(self, queue, pid,
                     start_index, end_index,
                     select_conn):
        """
        Here we process the each process into 10 multiple threads to do data process
        """
        m.info('Started processing record from %s to %s' % (start_index, end_index))

        threads_array = self.get_threads(start_index,
                                         end_index,
                                         10)

        for tid in range(1, 11):
            worker = threading.Thread(target=self.process_thread,
                                      args=(queue,
                                            pid,
                                            tid,
                                            threads_array[tid-1][0],
                                            threads_array[tid-1][1],
                                            select_conn.cursor(),
                                            threading.Lock()))

            worker.daemon = True
            worker.start()
            worker.join()

    @m.timing
    def process_thread(self, queue, pid, tid,
                       start_index, end_index,
                       sel_cur, lock):
        """
        Thread read data from database and doing the elatic search to get
        experience have the same data
        """
        sel_cur.execute(self.str_sql, (int(start_index), int(end_index)))
        sel_cur.close()
        message_txt = ('\t pid {:2}, tid {:2}, '
                       'start_index {:7}, end_index {:7}. ').format(pid,
                                                                    tid,
                                                                    start_index,
                                                                    end_index)
        print(message_txt, end=' ')


class PostgreSQLCommon():
    """ Simple working with database """
    def __init__(self):
        self.conn = psycopg2.connect(DB_URL)

    def query(self, query, **kwargs):
        """ Query executing for many records """
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, kwargs)
            return cur.fetchall()

    def query_one(self, query, **kwargs):
        """ Query executing for one record """
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, kwargs)
            return cur.fetchone()

    def execute(self, query, **kwargs):
        """ DML with transaction """
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute(query, kwargs)
            self.conn.commit()
            cur.close()

    def bulk_copy(self, file_source, target_table):
        """ Massive insertion """
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.copy_from(file_source, target_table, sep="\t")
            self.conn.commit()
            cur.close()

    def close(self):
        """ Connection closing """
        if self.conn:
            self.conn.close()
