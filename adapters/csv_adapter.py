#!/usr/bin/env python3

""" Working with CSV file """

import os
import multiprocessing as mp
import hashlib

from utils.monitoring import Monitoring
from adapters.database_tool import PostgreSQLCommon

m = Monitoring('csv_adapter')


class CsvAdapter:
    """ Class for the reading of CSV """
    def __init__(self, **kwargs):
        self.file_name_raw = kwargs['file_name_raw']
        self.file_end = os.path.getsize(self.file_name_raw)
        self.file_end_mb = self.get_size_in_mb(self.file_end)

        self.schema_target = kwargs['schema_target']
        self.file_name_hash = kwargs['file_name_hash']

        self.storage_table = '.'.join([self.schema_target, kwargs['storage_table']])
        self.chunk_counter = 0

    @staticmethod
    def md5(input_string):
        """ Get hash from a string """
        return hashlib.md5(input_string.encode('utf-8')).hexdigest()

    def get_hash(self, arr):
        """ Return the output hash-row """
        output_str = ('csv_adapter\t' +
                      arr[0] + '\t' +
                      self.md5(self.md5(arr[1]) +
                               self.md5(arr[2]) +
                               self.md5(arr[3]) +
                               self.md5(arr[4])))
        return output_str

    def process(self, line):
        """ Save hashed line into file """
        arr_line = list(line.split('\t'))
        hash_line = self.get_hash(arr_line)

        with open(self.file_name_hash, 'a') as hash_txt:
            hash_txt.write(hash_line + '\n')

    @staticmethod
    def get_size_in_mb(file_size):
        """ Return the size of file in Mb """
        return round(file_size / (1024 * 1024), 2)

    @m.timing
    def process_wrapper(self, chunk_start, chunk_size):
        """ Read a particular chunk """
        with open(self.file_name_raw, newline='\n') as file:
            file.seek(chunk_start)
            lines = file.read(chunk_size).splitlines()
            for line in lines:
                self.process(line)

        message_txt = (('\tReading from {:7} Mb to {:7} Mb (total: {} Mb). ')
                       .format(self.get_size_in_mb(chunk_start),
                               self.get_size_in_mb(chunk_start + chunk_size),
                               self.file_end_mb))
        print(message_txt, end='')

    def chunkify(self, size=1024*1024*5):
        """ Return a new chunk """
        with open(self.file_name_raw, 'rb') as file:
            chunk_end = file.tell()
            while True:
                chunk_start = chunk_end
                file.seek(size, 1)
                file.readline()
                chunk_end = file.tell()

                if chunk_end > self.file_end:
                    chunk_end = self.file_end
                    yield chunk_start, chunk_end - chunk_start
                    break
                else:
                    yield chunk_start, chunk_end - chunk_start

    @m.timing
    def run_reading(self):
        """ The main method for the reading """
        # init objects
        pool = mp.Pool(mp.cpu_count())
        jobs = []

        m.info('Run csv reading...')
        # create jobs
        for chunk_start, chunk_size in self.chunkify():
            jobs.append(pool.apply_async(self.process_wrapper,
                                         (chunk_start, chunk_size)))

        # wait for all jobs to finish
        for job in jobs:
            job.get()

        # clean up
        pool.close()
        pool.join()

        m.info('CSV file reading has been completed')

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def bulk_copy_to_db(self):
        """ Saving the hashed data into the database """
        database = PostgreSQLCommon()

        try:
            file = open(self.file_name_hash)
            database.bulk_copy(file, self.storage_table)

            m.info('Bulk insert from %s has been successfully completed!'
                   % self.file_name_hash)
        except Exception as err:
            m.error('OOps! Bulk insert operation FAILED! Reason: %s' % str(err))
        finally:
            database.close()

            if os.path.exists(self.file_name_hash):
                os.remove(self.file_name_hash)
