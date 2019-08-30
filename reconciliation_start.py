#!/usr/bin/env python3

import datetime
import time
import hashlib

import psycopg2
import psycopg2.extras

from adapters.postgresql_adapter import PostgreSQLAdapter
from adapters.csv_adapter import CsvAdapter

class Reconciliator:
    def __init__(self):
        # Unique table name for the parallel processing
        self.storage_table = "storage_" + str(int(time.time()))
        self.psa = PostgreSQLAdapter(self.storage_table)
        self.csv = CsvAdapter(self.storage_table, "data/transaction_data.csv")

    def storage_preparing(self):
        self.psa.storage_create()

    def postgresql_adapter_run(self):
        # self.psa.adapter_run()
        self.psa.adapter_run_main()

    def csv_adapter_run(self):
        self.csv.run_reading()
        self.csv.bulk_coly_to_db()

    def get_report(self):
        self.psa.get_discrepancy_report()

    def reconcillation_run(self):
        self.psa.save_clean_data()
        self.psa.drop_storage()


def main():
    _start = time.time()

    recon = Reconciliator()
    recon.storage_preparing()
    recon.postgresql_adapter_run()
    recon.csv_adapter_run()
    recon.get_report()
    recon.reconcillation_run()

    operation_took = round(time.time() - _start, 2)
    print('---> Total Processing time {0} seconds'.format(operation_took))

if __name__ == "__main__":
    main()
