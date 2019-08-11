#!/usr/bin/env python3


import os, sys
from random import randrange
from random import randint, choice
import uuid
import datetime
import time


def random_date(start, l):
    current = start
    while l >= 0:
        curr = current + \
            datetime.timedelta(days=randrange(365)) + \
            datetime.timedelta(hours=randrange(60)) + \
            datetime.timedelta(minutes=randrange(60)) + \
            datetime.timedelta(seconds=randrange(60))
        yield curr
        l -= 1

# Return a list of accounts
def get_accounts_num(acc_count):
    acc_list = []
    while acc_count >= 0:
        acc_list.append(uuid.uuid4())
        acc_count -= 1
    return acc_list


if __name__ == '__main__':

    if len(sys.argv) > 1:
        num_rows = int(sys.argv[1])
    else:
        num_rows = 1000

    if os.path.exists("data/transaction_data.csv"):
        os.remove("data/transaction_data.csv")

    myfile = open("data/transaction_data.csv", "w")
    startDate = datetime.datetime(2015, 1, 1, 12, 00)
    # Ten random accounts for the dummy data
    list_acc = get_accounts_num(10)
    list_type_deal = ['commision', 'deal']

    start_time = time.time()


    for i in range(num_rows):
        transaction_uid  = uuid.uuid4()
        account_uid = choice(list_acc)
        transaction_date = random_date(startDate, 0).__next__().strftime("%Y-%m-%d %H:%M:%S")
        type_deal = choice(list_type_deal)
        transaction_amount = randint(-1000, 1000)

        myfile.write("{0}\t{1}\t{2}\t{3}\t{4}\n".format(\
            transaction_uid,
            account_uid,
            transaction_date,
            type_deal,
            transaction_amount))

    myfile.close()
    operation_took = round(time.time() - start_time, 2)
    print("Test data creating for {0} rows took {1} seconds".format(num_rows, operation_took))
