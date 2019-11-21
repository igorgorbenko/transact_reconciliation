#!/usr/bin/env python3

""" Test data generating """

import os
import sys
import configparser

from random import randrange
from random import randint, choice
import uuid
import datetime as dt

from utils.monitoring import Monitoring as m

# FILE_NAME = 'data/transaction_data.csv'
# INITIAL_DATE = datetime.datetime(2015, 1, 1, 12, 00)
# RANDOM_ACCOUNTS = 10
# LIST_TYPE_DEAL = ['commision', 'deal']


class TestDataCreator:
    """ The main class for the data generating """
    def __init__(self, num_rows):
        self.config = configparser.ConfigParser()
        self.config.read('./conf/db.ini')

        self.data_file = self.config.get('CSV', 'file_name_raw')
        if os.path.exists(self.data_file):
            os.remove(self.data_file)

        self.date_in = dt.datetime.strptime(self.config.get('MAIN', 'initial_data'),
                                            '%Y-%m-%d')
        self.random_accounts_count = int(self.config.get('MAIN', 'random_accounts'))

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

    @m.timing
    def create_test_data(self):
        """ Generating and saving to the file """
        data_file = open(self.data_file, 'w')
        date_in = self.date_in
        list_acc = self.get_accounts_num()     # Ten random accounts

        for _ in range(self.num_rows):
            transaction_uid = uuid.uuid4()
            account_uid = choice(list_acc)
            transaction_date = (self.random_date(date_in, 0)
                                .__next__()
                                .strftime('%Y-%m-%d %H:%M:%S'))
            type_deal = choice(self.list_type_deal)
            transaction_amount = randint(-1000, 1000)

            data_file.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(
                transaction_uid,
                account_uid,
                transaction_date,
                type_deal,
                transaction_amount))

        data_file.close()
        print('Test data creating for {0} rows. '.format(self.num_rows), end=' ')


if __name__ == '__main__':

    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])
    else:
        num_rows = 1000

    # if os.path.exists(FILE_NAME):
    #     os.remove(FILE_NAME)

    tdc = TestDataCreator(num_rows)
    tdc.create_test_data()
