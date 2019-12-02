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

import psycopg2
from psycopg2 import sql

from adapters.database_tool import PostgreSQLCommon
from utils.monitoring import Monitoring


m = Monitoring('data_generating')

class TestDataCreator:
    """ The main class for the data generating """
    def __init__(self, num_rows, config, data_file):
        self.num_rows = num_rows
        self.config = config
        self.data_file = data_file
        if os.path.exists(self.data_file):
            os.remove(self.data_file)

        self.date_in = dt.datetime.strptime(self.config.get('MAIN', 'initial_date'),
                                            '%Y-%m-%d')
        self.random_accounts_count = int(self.config.get('MAIN', 'random_accounts'))
        self.list_acc = self.get_accounts_num()     # Ten random accounts

        self.list_type_deal = ['commision', 'deal']

    @staticmethod
    def get_random_date(start, num):
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

    def write_in_file(self, rows, chunk_start, chunk_end):
        """ Save data into csv file """
        try:
            with open(self.data_file, 'a', newline='\n') as file:
                csv_writer = csv.writer(file, delimiter='\t', )
                csv_writer.writerows(rows)
                file.flush()

            print('\tTest data created from {:7} to {:7} rows. '.format(chunk_start,
                                                                        chunk_end), end=' ')
        except Exception as err:
            m.error("OOps! File write function failed! Reason: %s'" % str(err))

    @m.timing
    def generate_test_data_by_chunk(self, chunk_start, chunk_end):
        """ Generating and saving to the file """
        num_rows_mp = chunk_end - chunk_start
        new_rows = []

        for _ in range(num_rows_mp):
            transaction_uid = uuid.uuid4()
            account_uid = choice(self.list_acc)
            transaction_date = (self.get_random_date(self.date_in, 0)
                                .__next__()
                                .strftime('%Y-%m-%d %H:%M:%S'))
            type_deal = choice(self.list_type_deal)
            transaction_amount = randint(-1000, 1000)

            new_rows.append([transaction_uid,
                             account_uid,
                             transaction_date,
                             type_deal,
                             transaction_amount])

        self.write_in_file(new_rows, chunk_start, chunk_end)


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

    def run_csv_writing(self):
        """ Writing the test data into csv file """
        pool = mp.Pool(mp.cpu_count() + 2)
        jobs = []

        for chunk_start, chunk_end in self.get_threads(0, self.num_rows):
            jobs.append(pool.apply_async(self.generate_test_data_by_chunk,
                                         (chunk_start, chunk_end)))
        #wait for all jobs to finish
        for job in jobs:
            job.get()

        #clean up
        pool.close()
        pool.join()


class GenerateTestData:
    """ The main class for creating a dummy data """

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read('./conf/db.ini')

        self.data_file = self.config.get('CSV', 'file_name_raw')
        self.schema_raw = self.config.get('POSTGRESQL', 'transaction_db_raw')
        self.raw_table_name = 'transaction_log'
        self.raw_full_table_name = '.'.join([self.schema_raw,
                                             self.raw_table_name])

        self.database = PostgreSQLCommon()


    @m.wrapper(m.entering, m.exiting)
    def create_db_schema(self):
        """ Execute a script schema_transaction.sql """
        sql_script_path = 'sql/schema_transaction.sql'

        if os.path.exists(sql_script_path):
            with open('sql/schema_transaction.sql', 'r') as file:
                sql_command = file.read()

                try:
                    self.database.execute(sql_command)
                    m.info('SQL script %s has been successfully executed!' % sql_script_path)
                except psycopg2.Error as err:
                    m.error('OOps! Script executing FAILED! Reason: %s' % str(err.pgerror))
        else:
            m.error('Oops! No such file %s' % sql_script_path)
            raise FileNotFoundError

    @staticmethod
    def create_folder(dir_name):
        """ Create a folder for the csv-data keeping """
        try:
            if not os.path.exists(dir_name):
                os.makedirs(dir_name)
            m.info('Directory %s has been created!' % dir_name)
        except Exception as err:
            m.error('Oops! Folder creating FAILED! Reason: %s' % err)

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def create_csv_file(self, num_rows, ):
        """ Create csv file """
        csv_creator = TestDataCreator(num_rows, self.config, self.data_file)
        csv_creator.run_csv_writing()

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def bulk_copy_to_db(self):
        """ Insert data into DB """
        columns = (
            'transaction_uid',
            'account_uid',
            'transaction_date',
            'type_deal',
            'transaction_amount'
        )

        try:
            with open(self.data_file, 'r') as csv_file:
                self.database.bulk_copy(csv_file,
                                        self.raw_full_table_name,
                                        columns)
                m.info('Bulk copy process into %s has been successfully executed!'
                       % self.raw_full_table_name)
        except psycopg2.Error as err:
            m.error('OOps! Bulk copy process FAILED! Reason: %s' % err.pgerror)

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def random_delete_rows(self):
        """ Random deleting some rows from the table """
        sql_command = sql.SQL("""
                        delete from {0}.{1}
                        where ctid = any(array(
                          select ctid
                          from {0}.{1}
                          tablesample bernoulli (1)
                          ))""").format(sql.Identifier(self.schema_raw),
                                        sql.Identifier(self.raw_table_name))
        try:
            rows = self.database.execute(sql_command)
            m.info('Has been deleted [%s rows] from table %s' % (rows, self.raw_table_name))
        except psycopg2.Error as err:
            m.error('Oops! Delete random rows has been FAILED. Reason: %s' % err.pgerror)

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def random_update_rows(self):
        """ Random update some rows from the table """
        sql_command = sql.SQL("""
                update {0}.{1}
                set transaction_amount = round(random()::numeric, 2)
                where ctid = any(array(
                  select ctid
                  from {0}.{1}
                  tablesample bernoulli (1) ))""").format(sql.Identifier(self.schema_raw),
                                                          sql.Identifier(self.raw_table_name))
        try:
            rows = self.database.execute(sql_command)
            m.info('Has been updated [%s rows] from table %s' % (rows, self.raw_table_name))
        except psycopg2.Error as err:
            m.error('Oops! Delete random rows has been FAILED. Reason: %s' % err.pgerror)

    def run(self, num_rows):
        """ Run the proceess """
        self.create_db_schema()
        self.create_folder('data')
        self.create_csv_file(num_rows)
        self.bulk_copy_to_db()
        self.random_delete_rows()
        self.random_update_rows()

@m.timing
def main():
    """ Data creating """
    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])
    else:
        num_rows = 100000

    gtd = GenerateTestData()
    m.info('START!')
    gtd.run(num_rows)
    m.info('END!')

if __name__ == '__main__':
    main()
