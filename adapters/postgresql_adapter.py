#!/usr/bin/env python3
import time

from .database_tool import MyDatabasePostgresql, PostgreSQLMultiThread


class PostgreSQLAdapter(MyDatabasePostgresql):
    def __init__(self, table_storage):
        self.table_storage = table_storage

    def storage_create(self):
        db = MyDatabasePostgresql()
        strSQL = """
            drop table if exists reconciliation_db.{0};
            create table reconciliation_db.{0} (
                adapter_name        varchar(50) not null,
                transaction_uid     uuid not null,
                hash                uuid not null,
                constraint pk_storage_{0} primary key (adapter_name, transaction_uid, hash)
            );
            create index {0}_hash_idx on reconciliation_db.{0} (hash);
            """.format(self.table_storage)

        try:
            db.execute(strSQL)
            print("---> Table", self.table_storage, "has been created!")
        except Exception as e:
            print("---> OOps! Table creating for Storage FAILED! Reason: ", str(e))
        finally:
            db.close()

    def drop_storage(self):
        db = MyDatabasePostgresql()
        strSQL = """
            drop table if exists reconciliation_db.{0};""".format(self.table_storage)

        try:
            db.execute(strSQL)
            print("---> Table", self.table_storage, "has been droped!")
        except Exception as e:
            print("---> OOps! Table droping for Storage",
                self.table_storage, "FAILED! Reason: ", str(e))
        finally:
            db.close()


    def adapter_run(self):
        db = MyDatabasePostgresql()

        strSQL = """
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
            insert into reconciliation_db.{0}
                (adapter_name, transaction_uid, hash)
            select
            	s.adapter_name,
            	s.transaction_uid,
            	s.hash::uuid
            from pre_select s;""".format(self.table_storage)

        try:
            start_time = time.time()
            db.execute(strSQL)
            operation_took = round(time.time() - start_time, 2)

            print("---> PostgreSQLAdapter.adapter_run successfully completed!",
                "Operation took", operation_took, "seconds")
        except Exception as e:
            print("---> OOps! PostgreSQLAdapter.adapter_run FAILED! Reason: ", str(e))
            print(strSQL)
        finally:
            db.close()


    def getRowCounts(self):
        rows_count = 0

        db = MyDatabasePostgresql()

        strSQL = """
            select count(*)
            from transaction_db_raw.transaction_log;"""

        try:
            rows = db.query(strSQL).fetchone()
            rows_count = rows[0]
        except Exception as e:
            print("---> OOps! PostgreSQLAdapter.getRowCounts FAILED! Reason: ", str(e))
            print(strSQL)
        finally:
            db.close()

        return rows_count


    def adapter_run_main(self):
        rows_count = self.getRowCounts()

        #  Not to large
        if rows_count < 100000:
            # Simple processing
            self.adapter_run()
        else:
            strSQL = """
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
                insert into reconciliation_db.{0}
                	(adapter_name, transaction_uid, hash)
                select
                	s.adapter_name,
                	s.transaction_uid,
                	s.hash::uuid
                from pre_select s;""".format(self.table_storage)

            start_time = time.time()

            print("---> Run multiprocessing read...")
            multi_run = PostgreSQLMultiThread(strSQL, rows_count)

        # try:
            #Creating database connection pool to help connection shared along process
            multi_run.create_connection_pool()
            multi_run.read_data()

            operation_took = round(time.time() - start_time, 2)
            print("---> PostgreSQLAdapter.PostgreSQLMultiThread.read_data",
                "successfully completed! Operation took", operation_took, "seconds")

        # except Exception as e:
        #     print("---> OOps! PostgreSQLAdapter.PostgreSQLMultiThread.read_data",
        #         "FAILED! Reason: ", str(e))
        #     print(strSQL)


    def get_discrepancy_report(self):
        db = MyDatabasePostgresql()

        strSQL = """
            select
            	s1.adapter_name,
            	count(s1.transaction_uid) as tran_count
            from reconciliation_db.{0} s1
            full join reconciliation_db.{0} s2
            	on s2.transaction_uid = s1.transaction_uid
            	and s2.adapter_name != s1.adapter_name
            	and s2.hash = s1.hash
            where s2.transaction_uid is null
            group by s1.adapter_name;""".format(self.table_storage)

        try:
            start_time = time.time()
            rows = db.query(strSQL).fetchall()
            operation_took = round(time.time() - start_time, 2)

            print("---> PostgreSQLAdapter.get_discrepancy_report successfully completed!",
                "Operation took", operation_took, "seconds")

            print("\nNumber of discrepancies detected by adapters")
            print('---------------------------------')
            for row in rows:
                print(row)
            print('---------------------------------')

        except Exception as e:
            print("---> OOps! PostgreSQLAdapter.get_discrepancy_report FAILED! Reason: ", str(e))
            print(strSQL)
        finally:
            db.close()


    def save_clean_data(self):
        db = MyDatabasePostgresql()

        strSQL = """
            with reconcil_data as (
            	select
            		s1.transaction_uid
            	from reconciliation_db.{0} s1
            	join reconciliation_db.{0} s2
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
            """.format(self.table_storage)

        try:
            start_time = time.time()
            db.execute(strSQL)
            operation_took = round(time.time() - start_time, 2)

            print("---> PostgreSQLAdapter.save_clean_data successfully completed!",
                "Operation took", operation_took, "seconds")
        except Exception as e:
            print("---> OOps! PostgreSQLAdapter.save_clean_data FAILED! Reason: ", str(e))
            print(strSQL)
        finally:
            db.close()
