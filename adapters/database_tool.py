#!/usr/bin/env python3
import time
import threading
from multiprocessing import Process
from multiprocessing import Queue
import configparser

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool


config = configparser.ConfigParser()
config.read('./conf/db.ini')

db_url = config.get('POSTGRESQL', 'db_url')


class PostgreSQLMultiThread:

    _select_conn_count = 10;
    _select_conn_pool = None;

    data_queque = Queue()  # reader reads data from queue


    def __init__(self, strSQL, totalRecords):
        # self = self;
        self.strSQL  = strSQL
        self.totalRecords = totalRecords


    def create_connection_pool(self):
        """ Create the thread safe threaded postgres connection pool"""

        # calculate the max and min connection required
        max_conn = self._select_conn_count;
        min_conn = max_conn / 2;

        # creating separate connection for read and write purpose
        self._select_conn_pool \
            = ThreadedConnectionPool(min_conn, max_conn, db_url);


    def chunks(self, l, start, n):
        """Yield successive n-sized chunks from l."""
        for i in range(start, len(l), n):
            # Bprint(i ,n)
            yield l[i:i + n]

    def get_threads(self, start=0, num=1000, div=10):
        inter = (num - start) // div
        mod = num % div
        threads_arr = []

        gener_list = list(self.chunks(range(0, num), start, inter + mod))

        for gen in gener_list:
            threads_arr.append([gen.start, gen.stop])

        return threads_arr


    def read_data(self):
        """
        This read thedata from the postgres and shared those records with each
        processor to perform their operation using threads
        Here we calculate the pardition value to help threading to read data from database
        """
        threads_array = self.get_threads(0, self.totalRecords, 10) # Its total record

        for pid in range(1, 11):
            # Getting connection from the connection pool
            select_conn = self._select_conn_pool.getconn();
            select_conn.autocommit = 1

            #Creating 10 process to perform the operation
            ps = Process(target=self.process_data,
                        args=(self.data_queque,
                            pid,
                            threads_array[pid-1][0],
                            threads_array[pid-1][1],
                            select_conn))

            ps.daemon = True;
            ps.start();
            _start = time.time()
            ps.join()
            print("Process %s took %s seconds" % (pid, (time.time() - _start)))


    def process_data(self, queue, pid, start_index, end_index, select_conn):
        """
        Here we process the each process into 10 multiple threads to do data process
        """
        print("\nStarted processing record from %s to %s" % (start_index, end_index))
        threads_array = self.get_threads(start_index, end_index, 10)

        for tid in range(1, 11):
            worker = threading.Thread(
                                    target=self.process_thread,
                                    args=(
                                        queue,
                                        pid,
                                        tid,
                                        threads_array[tid-1][0],
                                        threads_array[tid-1][1],
                                        select_conn.cursor(), None,
                                        threading.Lock())
                                    )

            worker.daemon = True;
            worker.start();
            worker.join()

    def process_thread(self, queue, pid, tid, start_index,
        end_index, sel_cur, ins_cur, lock):
        """
        Thread read data from database and doing the elatic search to get
        experience have the same data
        """
        sel_cur.execute(self.strSQL, (int(start_index), int(end_index)))
        print("\t", "pid", pid, "tid", tid, "start_index", start_index, "end_index", end_index)



class MyDatabasePostgresql():

    def __init__(self):

        self.conn = psycopg2.connect(db_url)
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def query(self, query):
        self.cur.execute(query)
        # = self.cur.fetchall()
        return self.cur

    def execute(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def bulk_copy(self, file_source, target_table):
        self.cur.copy_from(file_source, target_table, sep="\t")
        self.conn.commit()
        self.conn.close()

    def write(self, table, columns, data):
        query = "insert into {0} ({1}) values ({2});".format(table, columns, data)
        self.cursor.execute(query)

    def close(self):
        self.cur.close()
        self.conn.close()
        #print("PostgreSQL connection is closed")
