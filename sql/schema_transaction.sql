drop schema if exists transaction_db_raw cascade;
create schema transaction_db_raw

	create table transaction_log
        (
            id_num_row          bigserial,
            transaction_uid     UUID not null,
            account_uid         UUID,
            transaction_date    timestamp,
            type_deal           varchar(50),
            transaction_amount  float,
            constraint pk_transaction_log primary key (id_num_row)
        );
    create index transaction_log_id_idx
        on transaction_db_raw.transaction_log(id_num_row);

    cluster transaction_db_raw.transaction_log using transaction_log_id_idx;

    create unique index transaction_log_unique_idx
        on transaction_db_raw.transaction_log (transaction_uid);


drop schema if exists transaction_db_clean cascade;
create schema transaction_db_clean

	create table transaction_log
        (
            transaction_uid     UUID not null,
            account_uid         UUID,
            transaction_date    timestamp,
            type_deal           varchar(50),
            transaction_amount  float,
            date_load           timestamp default now(),
            constraint pk_transaction_log primary key (transaction_uid)
        )
    ;

    create index transaction_log_transaction_date_idx
        on transaction_db_clean.transaction_log(transaction_date);

    cluster transaction_db_clean.transaction_log using transaction_log_transaction_date_idx;

    create index transaction_log_account_uid_idx
        on transaction_db_clean.transaction_log (account_uid);

drop schema if exists reconciliation_db cascade;
create schema reconciliation_db;
