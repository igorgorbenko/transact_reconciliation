#!/usr/bin/env python3

""" Test data generating """

import os
import sys
from random import randrange
from random import randint, choice
import uuid
import datetime

from utils.monitoring import Monitoring as m

FILE_NAME = 'data/transaction_data.csv'
INITIAL_DATE = datetime.datetime(2015, 1, 1, 12, 00)
RANDOM_ACCOUNTS = 10
LIST_TYPE_DEAL = ['commision', 'deal']


class TestDataCreator:
    """ The main class for the data generating """
    def __init__(self, num_rows):
        self.data_file = FILE_NAME
        self.date_in = INITIAL_DATE
        self.list_type_deal = LIST_TYPE_DEAL
        self.num_rows = num_rows

    @staticmethod
    def random_date(start, num):
        """ Return a random date from a start value """
        current = start
        while num >= 0:
            curr = current + \
                   datetime.timedelta(days=randrange(365)) + \
                   datetime.timedelta(hours=randrange(60)) + \
                   datetime.timedelta(minutes=randrange(60)) + \
                   datetime.timedelta(seconds=randrange(60))
            yield curr
            num -= 1

    @staticmethod
    def get_accounts_num(acc_count):
        """ Return a list of accounts """
        acc_list = []
        while acc_count >= 0:
            acc_list.append(uuid.uuid4())
            acc_count -= 1
        return acc_list

    @m.timing
    def create_test_data(self):
        """ Generating and saving to the file """
        data_file = open(self.data_file, 'w')
        date_in = self.date_in
        list_acc = self.get_accounts_num(RANDOM_ACCOUNTS)     # Ten random accounts

        for _ in range(self.num_rows):
            transaction_uid = uuid.uuid4()
            account_uid = choice(list_acc)
            transaction_date = self.random_date(date_in, 0).__next__().strftime('%Y-%m-%d %H:%M:%S')
            type_deal = choice(self.list_type_deal)
            transaction_amount = randint(-1000, 1000)

            data_file.write('{0}\t{1}\t{2}\t{3}\t{4}\n'.format(\
                transaction_uid,
                account_uid,
                transaction_date,
                type_deal,
                transaction_amount))

        data_file.close()
        return {'log_txt': 'Test data creating for {0} rows'.format(self.num_rows)}


if __name__ == '__main__':

    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])
    else:
        num_rows = 1000

    if os.path.exists(FILE_NAME):
        os.remove(FILE_NAME)

    tdc = TestDataCreator(num_rows)
    tdc.create_test_data()
