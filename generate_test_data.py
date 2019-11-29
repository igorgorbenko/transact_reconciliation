#!/usr/bin/env python3

""" Test data generating """

import os
import sys
import csv
import configparser
import multiprocessing as mp

from random import randrange
from random import randint, choice
import uuid
import datetime as dt

from utils.monitoring import Monitoring


m = Monitoring('data_generating')

class TestDataCreator:
    """ The main class for the data generating """
    def __init__(self, num_rows):
        self.config = configparser.ConfigParser()
        self.config.read('./conf/db.ini')

        self.data_file = self.config.get('CSV', 'file_name_raw')
        if os.path.exists(self.data_file):
            os.remove(self.data_file)

        self.date_in = dt.datetime.strptime(self.config.get('MAIN', 'initial_date'),
                                            '%Y-%m-%d')
        self.random_accounts_count = int(self.config.get('MAIN', 'random_accounts'))
        self.list_acc = self.get_accounts_num()     # Ten random accounts

        self.list_type_deal = ['commision', 'deal']
        self.num_rows = num_rows

    @staticmethod
    def random_date(start, num):
        """ Return a random date from a start value """
        current = start
        while num >= 0:
            curr = current + \
                   dt.timedelta(days=randrange(365)) + \
                   dt.timedelta(hours=randrange(60)) + \
                   dt.timedelta(minutes=randrange(60)) + \
                   dt.timedelta(seconds=randrange(60))
            yield curr
            num -= 1

    def get_accounts_num(self):
        """ Return a list of accounts """
        acc_list = []
        while self.random_accounts_count >= 0:
            acc_list.append(uuid.uuid4())
            self.random_accounts_count -= 1
        return acc_list

    def write_in_file(self, rows):
        """ Save data into csv file """
        flag = True
        try:
            with open(self.data_file, 'a', newline='\n') as file:
                csv_writer = csv.writer(file, delimiter='\t', )
                csv_writer.writerows(rows)
                file.flush()
        except Exception as err:
            flag = False
            m.error("OOps! File write function failed! Reason: %s'" % str(err))

        return flag

    @m.timing
    def create_test_data_mp(self, chunk_start, chunk_end):
        """ Generating and saving to the file """
        num_rows_mp = chunk_end - chunk_start
        new_rows = []

        for _ in range(num_rows_mp):
            transaction_uid = uuid.uuid4()
            account_uid = choice(self.list_acc)
            transaction_date = (self.random_date(self.date_in, 0)
                                .__next__()
                                .strftime('%Y-%m-%d %H:%M:%S'))
            type_deal = choice(self.list_type_deal)
            transaction_amount = randint(-1000, 1000)

            new_rows.append([transaction_uid,
                             account_uid,
                             transaction_date,
                             type_deal,
                             transaction_amount])

        if self.write_in_file(new_rows):
            print('\tTest data created from {:7} to {:7} rows. '.format(chunk_start,
                                                                        chunk_end), end=' ')

    @staticmethod
    def chunks(array, start, num):
        """Yield successive n-sized chunks from array"""
        for i in range(start, len(array), num):
            yield array[i:i + num]

    @classmethod
    def get_threads(cls, start=0,
                    num=1000, div=100):
        """ Split input value into equal chunks """
        threads_arr = []

        if num >= 100000:
            div = num // 50000
            inter = (num - start) // div
            mod = num % div

            gener_list = list(cls.chunks(range(0, num), start, inter + mod))

            for gen in gener_list:
                threads_arr.append([gen.start, gen.stop])
        else:
            threads_arr.append([start, num])

        return threads_arr

    @m.wrapper(m.entering, m.exiting)
    def run_writing(self):
        """ Writing the test data into csv file """
        pool = mp.Pool(mp.cpu_count() + 2)
        jobs = []

        for chunk_start, chunk_end in self.get_threads(0,
                                                       self.num_rows):
            jobs.append(pool.apply_async(self.create_test_data_mp,
                                         (chunk_start, chunk_end)))
        #wait for all jobs to finish
        for job in jobs:
            job.get()

        #clean up
        pool.close()
        pool.join()

if __name__ == '__main__':

    num_rows = 100000

    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])

    tdc = TestDataCreator(num_rows)
    tdc.run_writing()
