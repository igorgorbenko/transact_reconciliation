#!/usr/bin/env python3

""" The main reconciliation process module"""

import time

from adapters.postgresql_adapter import PostgreSQLAdapter
from adapters.csv_adapter import CsvAdapter

from utils.monitoring import Monitoring as m


class Reconciliator:
    """ Reconciliation executor """
    def __init__(self):
        # Unique table name for the parallel processing
        self.storage_table = 'storage_' + str(int(time.time()))
        self.psa = PostgreSQLAdapter(self.storage_table,
                                     'transaction_db_raw',
                                     'reconciliation_db')
        self.csv = CsvAdapter(self.storage_table, 'data/transaction_data.csv')

    def storage_preparing(self):
        """ Database preparing """
        self.psa.storage_create()

    def postgresql_adapter_run(self):
        """ Postgre side preparing """
        self.psa.adapter_run_main()

    def csv_adapter_run(self):
        """ CSV side preparing """
        self.csv.run_reading()
        self.csv.bulk_coly_to_db()

    def get_report(self):
        """ Return the detailed report """
        self.psa.get_discrepancy_report()

    def reconcillation_run(self):
        """ Comparison the data from two sources """
        self.psa.save_clean_data()
        self.psa.drop_storage()

    def start_all(self):
        """ Run all steps """
        self.storage_preparing()
        self.postgresql_adapter_run()
        self.csv_adapter_run()
        self.get_report()
        self.reconcillation_run()

@m.timing
def main():
    """ Main starter """
    recon = Reconciliator()
    recon.start_all()
    return {'log_txt': '---> Processing has been completed'}

if __name__ == '__main__':
    main()
