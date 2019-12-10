#!/usr/bin/env python3
""" Postgres stuff """

import psycopg2
from psycopg2 import sql

from adapters.database_tool import PostgreSQLCommon, PostgreSQLMultiThread
from utils.monitoring import Monitoring


m = Monitoring('postgresql_adapter')


class PostgreSQLAdapter:
    """ The adapter for PostgreSQL """
    def __init__(self, **kwargs):
        self.storage_table = kwargs['storage_table']
        self.schema_raw = kwargs['schema_raw']
        self.schema_target = kwargs['schema_target']
        self.schema_db_clean = kwargs['schema_db_clean']
        self.rows_count = 0
        self.max_id_num_row = 0
        self.database = PostgreSQLCommon()

    def storage_create(self):
        """ Create a table for the comparing the sources """
        sql_command = sql.SQL("""
            drop table if exists {0}.{1};
            create table {0}.{1} (
                adapter_name        varchar(50) not null,
                transaction_uid     uuid not null,
                hash                uuid not null,
                constraint {2} primary key (adapter_name, transaction_uid, hash)
            );
            create index {3} on {0}.{1} (hash);
            """).format(sql.Identifier(self.schema_target),
                        sql.Identifier(self.storage_table),
                        sql.Identifier('_'.join(['pk', self.storage_table])),
                        sql.Identifier('_'.join([self.storage_table, 'hash_idx'])))
        try:
            self.database.execute(sql_command)
            m.info('Table %s has been created!' % self.storage_table)
        except psycopg2.Error as err:
            m.error('OOps! Table creating for Storage FAILED! Reason: %s' % str(err.pgerror))

    def drop_storage(self):
        """ Drop a staging table """
        sql_command = sql.SQL("""
            drop table if exists {0}.{1};
            """).format(sql.Identifier(self.schema_target),
                        sql.Identifier(self.storage_table))
        try:
            self.database.execute(sql_command)
            m.info('Table %s has been droped!' % self.storage_table)
        except psycopg2.Error as err:
            m.error('OOps! Table droping for Storage %s FAILED! Reason: %s'
                    % (self.storage_table, str(err.pgerror)))

    def adapter_simple_run(self):
        """ Insert hashed data from PostgreSQL """
        sql_command = sql.SQL("""
            with pre_select as (
                select
                    transaction_uid,
                    'postresql_adapter' as adapter_name,
                    md5(
                        coalesce(md5(account_uid::text), ' ') ||
                        coalesce(md5(to_char(transaction_date, 'YYYY-MM-DD HH24:MI:SS')), ' ') ||
                        coalesce(md5(type_deal::text), ' ') ||
                        coalesce(md5(transaction_amount::text), ' ')) as hash
                from {0}.transaction_log
            )
            insert into {1}.{2}
                (adapter_name, transaction_uid, hash)
            select
                s.adapter_name,
                s.transaction_uid,
                s.hash::uuid
            from pre_select s;""").format(sql.Identifier(self.schema_raw),
                                          sql.Identifier(self.schema_target),
                                          sql.Identifier(self.storage_table))

        try:
            self.database.execute(sql_command)
            m.info('PostgreSQL simple adapter_run successfully completed')
        except psycopg2.Error as err:
            m.error('OOps! PostgreSQL simple adapter_run FAILED! Reason %s' % str(err.pgerror))

    def get_rows_count(self):
        """ Return a count of rows """
        rows_count = 0
        max_id_num_row = 0

        sql_command = """
            select count(*), max(id_num_row)
            from transaction_db_raw.transaction_log;"""

        try:
            rows = self.database.query_one(sql_command)
            rows_count = rows[0]
            max_id_num_row = rows[1]
        except psycopg2.Error as err:
            m.error('OOps! PostgreSQLAdapter.adapter_run FAILED! Reason: %s, sql command: %s'
                    % (str(err.pgerror), sql_command))

        return rows_count, max_id_num_row

    def adapter_thread_run(self):
        """ Adapter running in multi-threads option """
        sql_command = sql.SQL("""
            with pre_select as (
                select
                    transaction_uid,
                    'postresql_adapter' as adapter_name,
                    md5(
                        coalesce(md5(account_uid::text), ' ') ||
                        coalesce(md5(to_char(transaction_date,
                            'YYYY-MM-DD HH24:MI:SS')), ' ') ||
                        coalesce(md5(type_deal::text), ' ') ||
                        coalesce(md5(transaction_amount::text), ' ')) as hash
                from {0}.transaction_log
                where id_num_row > %s and id_num_row <= %s
            )
            insert into {1}.{2}
                (adapter_name, transaction_uid, hash)
            select
                s.adapter_name,
                s.transaction_uid,
                s.hash::uuid
            from pre_select s;""").format(sql.Identifier(self.schema_raw),
                                          sql.Identifier(self.schema_target),
                                          sql.Identifier(self.storage_table))

        m.info('Run multiprocessing read...')
        multi_run = PostgreSQLMultiThread(sql_command, self.rows_count, self.max_id_num_row)

        # Creating database connection pool to help connection shared along process
        multi_run.create_connection_pool()
        multi_run.read_data()

        m.info('Read_data successfully completed')

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def adapter_run_main(self):
        """ Depending on the volume of input data,
            the necessary handler is launched """
        self.rows_count, self.max_id_num_row = self.get_rows_count()

        if self.rows_count < 100000:
            # Simple processing
            self.adapter_simple_run()
        else:
            self.adapter_thread_run()

    @m.wrapper(m.entering, m.exiting)
    def get_discrepancy_report(self):
        """ Reconciliation report returning """

        sql_command = sql.SQL("""
            select
                s1.adapter_name,
                count(s1.transaction_uid) as tran_count
            from {0}.{1} s1
            full join {0}.{1} s2
                on s2.transaction_uid = s1.transaction_uid
                and s2.adapter_name != s1.adapter_name
                and s2.hash = s1.hash
            where s2.transaction_uid is null
            group by s1.adapter_name;""").format(sql.Identifier(self.schema_target),
                                                 sql.Identifier(self.storage_table))

        try:
            rows = self.database.query(sql_command)

            m.info('Discrepancy_report successfully completed!')

            print('\n\tNumber of discrepancies detected by adapters')
            print('\t---------------------------------')
            for row in rows:
                print('\t', end='')
                print('{:20} | {:10}'.format(row[0], row[1]))
            print('\t---------------------------------')

        except psycopg2.Error as err:
            m.error('OOps! Get_discrepancy_report FAILED! Reason: %s' % str(err.pgerror))

    @m.timing
    @m.wrapper(m.entering, m.exiting)
    def save_clean_data(self):
        """ Saving the reconcilied date into """
        sql_command = sql.SQL("""
            with reconcil_data as (
                select
                    s1.transaction_uid
                from {0}.{1} s1
                join {0}.{1} s2
                    on s2.transaction_uid = s1.transaction_uid
                    and s2.adapter_name != s1.adapter_name
                where s2.hash = s1.hash
                    and s1.adapter_name = 'postresql_adapter'
            )
            insert into {2}.transaction_log
            select
                t.transaction_uid,
                t.account_uid,
                t.transaction_date,
                t.type_deal,
                t.transaction_amount
            from {3}.transaction_log t
            join reconcil_data r
                on t.transaction_uid = r.transaction_uid
            where not exists
                (
                    select 1
                    from {2}.transaction_log tl
                    where tl.transaction_uid = t.transaction_uid
                )
            """).format(sql.Identifier(self.schema_target),
                        sql.Identifier(self.storage_table),
                        sql.Identifier(self.schema_db_clean),
                        sql.Identifier(self.schema_raw))

        try:
            self.database.execute(sql_command)

            m.info('Saving to the clean schema has been successfully completed!')
        except psycopg2.Error as err:
            m.error('OOps! Save_clean_data FAILED! Reason: %s, SQL command: %s'
                    % (str(err.pgerror), sql_command))
