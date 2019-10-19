#!/usr/bin/env python3
""" Postgres stuff """

import psycopg2
from psycopg2 import sql

from adapters.database_tool import PostgreSQLCommon, PostgreSQLMultiThread
from utils.monitoring import Monitoring as m


class PostgreSQLAdapter:
    """ The adapter for PostgreSQL """
    def __init__(self, table_storage,
                 schema_raw, schema_target):
        self.table_storage = table_storage
        self.schema_raw = schema_raw
        self.schema_target = schema_target
        self.rows_count = 0
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
                        sql.Identifier(self.table_storage),
                        sql.Identifier('_'.join(['pk_storage', self.table_storage])),
                        sql.Identifier('_'.join([self.table_storage, 'hash_idx'])))
        try:
            self.database.execute(sql_command)
            print("---> Table", self.table_storage, "has been created!")
        except psycopg2.Error as err:
            print("---> OOps! Table creating for Storage FAILED! Reason: ", str(err))


    def drop_storage(self):
        """ Drop a staging table """
        sql_command = sql.SQL("""
            drop table if exists {0}.{1};
            """).format(sql.Identifier(self.schema_target),
                        sql.Identifier(self.table_storage))
        try:
            self.database.execute(sql_command)
            print("---> Table", self.table_storage, "has been droped!")
        except psycopg2.Error as err:
            print("---> OOps! Table droping for Storage",
                  self.table_storage, "FAILED! Reason: ", str(err))

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
            	from transaction_db_raw.transaction_log
            )
            insert into {0}.{1}
                (adapter_name, transaction_uid, hash)
            select
            	s.adapter_name,
            	s.transaction_uid,
            	s.hash::uuid
            from pre_select s;""").format(sql.Identifier(self.schema_target),
                                          sql.Identifier(self.table_storage))

        try:
            self.database.execute(sql_command)
            message_txt = "---> PostgreSQLAdapter.adapter_run successfully completed"
        except psycopg2.Error as err:
            message_txt = "---> OOps! PostgreSQLAdapter.adapter_run FAILED! Reason: ", str(err)

        return {'log_txt': message_txt}

    def get_rows_count(self):
        """ Return a count of rows """
        rows_count = 0

        sql_command = """
            select count(*)
            from transaction_db_raw.transaction_log;"""

        try:
            rows = self.database.query_one(sql_command)
            rows_count = rows[0]
            print("rows_count", rows_count, "rows", rows)
        except psycopg2.Error as err:
            print("---> OOps! PostgreSQLAdapter.get_rows_count FAILED! Reason: ", str(err))
            print(sql_command)

        return rows_count


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
                from transaction_db_raw.transaction_log
                where id_num_row > %s and id_num_row <= %s
            )
            insert into {0}.{1}
                (adapter_name, transaction_uid, hash)
            select
                s.adapter_name,
                s.transaction_uid,
                s.hash::uuid
            from pre_select s;""").format(sql.Identifier(self.schema_target),
                                          sql.Identifier(self.table_storage))

        print("---> Run multiprocessing read...")
        multi_run = PostgreSQLMultiThread(sql_command, self.rows_count)

        #Creating database connection pool to help connection shared along process
        multi_run.create_connection_pool()
        multi_run.read_data()

        message_txt = ("---> PostgreSQLAdapter.PostgreSQLMultiThread.read_data ",
                       "successfully completed")

        return {'log_txt': message_txt}


    @m.timing
    def adapter_run_main(self):
        """ Depending on the volume of input data,
            the necessary handler is launched """
        self.rows_count = self.get_rows_count()
        if self.rows_count < 100000:
            # Simple processing
            result = self.adapter_simple_run()
        else:
            result = self.adapter_thread_run()

        return result


    @m.timing
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
                                                 sql.Identifier(self.table_storage))

        try:
            rows = self.database.query(sql_command)

            message_txt = "---> PostgreSQLAdapter.get_discrepancy_report successfully completed"

            print("\nNumber of discrepancies detected by adapters")
            print('---------------------------------')
            for row in rows:
                print(row)
            print('---------------------------------')

        except psycopg2.Error as err:
            message_txt = ("---> OOps! PostgreSQLAdapter.get_discrepancy_report FAILED!",
                           "Reason:" + str(err))

        return {"log_txt": message_txt}


    @m.timing
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
            insert into transaction_db_clean.transaction_log
            select
            	t.transaction_uid,
            	t.account_uid,
            	t.transaction_date,
            	t.type_deal,
            	t.transaction_amount
            from transaction_db_raw.transaction_log t
            join reconcil_data r
            	on t.transaction_uid = r.transaction_uid
            where not exists
                (
                    select 1
                    from transaction_db_clean.transaction_log tl
                    where tl.transaction_uid = t.transaction_uid
                )
            """).format(sql.Identifier(self.schema_target),
                        sql.Identifier(self.table_storage))

        try:
            self.database.execute(sql_command)

            message_txt = "---> PostgreSQLAdapter.save_clean_data successfully completed"
        except psycopg2.Error as err:
            message_txt = ("---> OOps! PostgreSQLAdapter.save_clean_data FAILED!",
                           "Reason: " + str(err))
            print(sql_command)

        return {"log_txt": message_txt}
