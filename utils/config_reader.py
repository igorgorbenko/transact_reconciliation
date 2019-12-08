#!/usr/bin/env python3
""" Config reader module """

import configparser

class ConfigReader:
    """ Config reader class"""
    conf = {}

    def __init__(self, config_file):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.config_read()

    def config_read(self):
        """ Read the whole config file """
        self.conf['db_url'] = self.config.get('POSTGRESQL', 'db_url')
        self.conf['reconciliation_db'] = self.config.get('POSTGRESQL', 'reconciliation_db')
        self.conf['transaction_db_raw'] = self.config.get('POSTGRESQL', 'transaction_db_raw')
        self.conf['transaction_db_clean'] = self.config.get('POSTGRESQL', 'transaction_db_clean')

        self.conf['file_name_raw'] = self.config.get('CSV', 'file_name_raw')
        self.conf['file_name_hash'] = self.config.get('CSV', 'file_name_hash')

        self.conf['initial_date'] = self.config.get('MAIN', 'initial_date')
        self.conf['random_accounts'] = self.config.get('MAIN', 'random_accounts')

    def get_attr(self, attribute_name):
        """ Return the specific attribute """
        return self.conf.get(attribute_name)
