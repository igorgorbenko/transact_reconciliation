#!/usr/bin/env python3

""" The main reconciliation process module"""

import time

from adapters.postgresql_adapter import PostgreSQLAdapter
from adapters.csv_adapter import CsvAdapter
from utils.monitoring import Monitoring
from utils.config_reader import ConfigReader


m = Monitoring('reconciliation_main')


class Reconciliator:
    """ Reconciliation executor """
    def __init__(self):
        # Unique table name for the parallel processing
        self.conf_reader = ConfigReader('./conf/db.ini')

        self.storage_table = 'storage_' + str(int(time.time()))
        self.psa = PostgreSQLAdapter(storage_table=self.storage_table,
                                     schema_raw=self.conf_reader.get_attr('transaction_db_raw'),
                                     schema_target=self.conf_reader.get_attr('reconciliation_db'),
                                     schema_db_clean=self.conf_reader.get_attr('transaction_db_clean'))

        self.csv = CsvAdapter(storage_table=self.storage_table,
                              schema_target=self.conf_reader.get_attr('reconciliation_db'),
                              file_name_raw=self.conf_reader.get_attr('file_name_raw'),
                              file_name_hash=self.conf_reader.get_attr('file_name_hash'))

    def storage_preparing(self):
        """ Database preparing """
        self.psa.storage_create()

    def postgresql_adapter_run(self):
        """ Postgre side preparing """
        self.psa.adapter_run_main()

    @m.timing
    def csv_adapter_run(self):
        """ CSV side preparing """
        self.csv.run_reading()
        self.csv.bulk_copy_to_db()

    @m.timing
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
        self.csv_adapter_run()
        self.postgresql_adapter_run()
        self.get_report()
        self.reconcillation_run()


@m.timing
def main():
    """ Main starter """
    recon = Reconciliator()
    m.info('START!')
    recon.start_all()
    m.info('END!')

if __name__ == '__main__':
    main()
