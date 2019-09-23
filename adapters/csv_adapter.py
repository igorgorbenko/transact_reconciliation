#!/usr/bin/env python3

""" Working with CSV file """

import os
import multiprocessing as mp
import hashlib

from utils.monitoring import Monitoring as m
from database_tool import PostgreSQLCommon


class CsvAdapter:
    """ Class for the reading of CSV """
    def __init__(self, table_storage, file_name):
        self.file_name = file_name
        self.table_storage = "reconciliation_db." + table_storage
        self.file_name_hash = "data/transaction_hashed.csv"

    @staticmethod
    def md5(input_string):
        """ Get hash from a string """
        return hashlib.md5(input_string.encode('utf-8')).hexdigest()

    def get_hash(self, arr):
        """ Return the output hash-row """
        output_str = 'csv_adapter\t' + \
                        arr[0] + '\t' + \
                        self.md5(
                            self.md5(arr[1]) + \
                            self.md5(arr[2]) + \
                            self.md5(arr[3]) + \
                            self.md5(arr[4])
                            )
        return output_str

    def process(self, line):
        """ Save hashed line into file """
        arr_line = list(line.split('\t'))
        hash_line = self.get_hash(arr_line)

        with open(self.file_name_hash, "a") as hash_txt:
            hash_txt.write(hash_line + '\n')

    def process_wrapper(self, chunk_start, chunk_size):
        """ Read a particular chunk """
        with open(self.file_name) as file:
            file.seek(chunk_start)
            lines = file.read(chunk_size).splitlines()
            for line in lines:
                self.process(line)

    def chunkify(self, size=1024*1024):
        """ Return a new chunk """
        file_end = os.path.getsize(self.file_name)
        with open(self.file_name, 'rb') as file:
            chunk_end = file.tell()
            while True:
                chunk_start = chunk_end
                file.seek(size, 1)
                file.readline()
                chunk_end = file.tell()
                yield chunk_start, chunk_end - chunk_start
                if chunk_end > file_end:
                    break

    @m.timing
    def run_reading(self):
        """ The main method fo the reading """
        #init objects
        pool = mp.Pool(4)
        jobs = []

        #create jobs
        for chunk_start, chunk_size in self.chunkify():
            jobs.append(pool.apply_async(self.process_wrapper,
                                         (chunk_start, chunk_size)))

        #wait for all jobs to finish
        for job in jobs:
            job.get()

        #clean up
        pool.close()
        return {'log_txt' : "---> CsvAdapter.run_reading has been completed"}

    @m.timing
    def bulk_coly_to_db(self):
        """ Saving the hashed data into the database """
        database = PostgreSQLCommon()

        try:
            file = open(self.file_name_hash)
            database.bulk_copy(file, self.table_storage)

            message_txt = "---> Bulk insert from" + \
                          self.file_name_hash + \
                          "successfully completed!"

            if os.path.exists("data/transaction_hashed.csv"):
                os.remove("data/transaction_hashed.csv")

        except Exception as err:
            message_txt = "---> OOps! Bulk insert operation FAILED! Reason: ", str(err)
        finally:
            database.close()

        return {'log_txt': message_txt}
