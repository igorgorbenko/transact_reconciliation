#!/usr/bin/env python3

import datetime
import time
import hashlib

import psycopg2
import psycopg2.extras

from adapters.postgresql_adapter import PostgreSQLAdapter
from adapters.csv_adapter import CsvAdapter

from utils.monitoring import Monitoring as m


class Reconciliator:
    def __init__(self):
        # Unique table name for the parallel processing
        self.storage_table = "storage_" + str(int(time.time()))
        self.psa = PostgreSQLAdapter(self.storage_table)
        self.csv = CsvAdapter(self.storage_table, "data/transaction_data.csv")

    def storage_preparing(self):
        self.psa.storage_create()

    def postgresql_adapter_run(self):
        self.psa.adapter_run_main()

    def csv_adapter_run(self):
        self.csv.run_reading()
        self.csv.bulk_coly_to_db()

    def get_report(self):
        self.psa.get_discrepancy_report()

    def reconcillation_run(self):
        self.psa.save_clean_data()
        self.psa.drop_storage()

    def start_all(self):
        self.storage_preparing()
        self.postgresql_adapter_run()
        self.csv_adapter_run()
        self.get_report()
        self.reconcillation_run()


@m.timing
def main():
    recon = Reconciliator()
    recon.start_all()
    return {'log_txt': '---> Processing has been completed'}

if __name__ == "__main__":
    main()
