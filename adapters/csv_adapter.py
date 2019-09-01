#!/usr/bin/env python3

import time
import multiprocessing as mp,os
import hashlib

from .database_tool import PostgreSQLCommon
from utils.monitoring import Monitoring as m


class CsvAdapter:
    def __init__(self, table_storage, file_name):
        self.file_name = file_name
        self.table_storage = "reconciliation_db." + table_storage
        self.file_name_hash = "data/transaction_hashed.csv"

    def md5(self, str):
        return hashlib.md5(str.encode('utf-8')).hexdigest()

    def get_hash(self, arr):
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
        arr_line = list(line.split('\t'))
        hast_line = self.get_hash(arr_line)

        with open(self.file_name_hash, "a") as hash_txt:
            hash_txt.write(hast_line + '\n')


    def process_wrapper(self, chunkStart, chunkSize):
        with open(self.file_name) as f:
            f.seek(chunkStart)
            lines = f.read(chunkSize).splitlines()
            for line in lines:
                self.process(line)


    def chunkify(self, size=1024*1024):
        fileEnd = os.path.getsize(self.file_name)
        with open(self.file_name,'rb') as f:
            chunkEnd = f.tell()
            while True:
                chunkStart = chunkEnd
                f.seek(size, 1)
                f.readline()
                chunkEnd = f.tell()
                yield chunkStart, chunkEnd - chunkStart
                if chunkEnd > fileEnd:
                    break

    @m.timing
    def run_reading(self):
        #init objects
        pool = mp.Pool(4)
        jobs = []

        #create jobs
        for chunkStart,chunkSize in self.chunkify():
            jobs.append(pool.apply_async(self.process_wrapper,(chunkStart,chunkSize)))

        #wait for all jobs to finish
        for job in jobs:
            job.get()

        #clean up
        pool.close()

        return {'log_txt' : "---> CsvAdapter.run_reading has been completed"}

    @m.timing
    def bulk_coly_to_db(self):
        db = PostgreSQLCommon()

        try:
            f = open(self.file_name_hash)
            db.bulk_copy(f, self.table_storage)

            message_txt = "---> Bulk insert from" + \
                          self.file_name_hash + \
                          "successfully completed!"

            if os.path.exists("data/transaction_hashed.csv"):
                os.remove("data/transaction_hashed.csv")

        except Exception as e:
            message_txt = "---> OOps! Bulk insert operation FAILED! Reason: ", str(e)
        finally:
            db.close()

        return {'log_txt': message_txt}
