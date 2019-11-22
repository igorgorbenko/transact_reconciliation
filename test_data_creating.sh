#!/bin/bash

ROWS=$1

if [ "$#" -ne 1 ]; then
    ROWS=10000
fi

getValue()
{
    section="$1"
    param="$2"
    found=false
    while read line
    do
        [[ $found == false && "$line" != "[$section]" ]] &&  continue
        [[ $found == true && "${line:0:1}" = '[' ]] && break
        found=true
        [[ "${line%=*}" == "$param" ]] && { echo "${line#*=}"; break; }
    done
}

POSTGRESQL_CONN=$(getValue POSTGRESQL db_url < conf/db.ini)


psql $POSTGRESQL_CONN -a -f sql/schema_transaction.sql  >/dev/null 2>&1

# create new text file with random data
echo "------- Start table cleaning... -------"
psql $POSTGRESQL_CONN  -c \
    "truncate table transaction_db_raw.transaction_log"

echo "------- Start test data creating... -------"
python3 ./generate_test_data.py $ROWS
sleep 1

echo "------- Start data loading... -------"
psql $POSTGRESQL_CONN  -c \
    """\\copy
    transaction_db_raw.transaction_log(
        transaction_uid,
        account_uid,
        transaction_date,
        type_deal,
        transaction_amount)
    FROM 'data/transaction_data.csv'
    WITH (format 'csv', delimiter E'\t')"""

echo "------- Start delete random rows from table... -------"
psql $POSTGRESQL_CONN  -c \
    "delete from transaction_db_raw.transaction_log
    where ctid = any(array(
      select ctid
      from transaction_db_raw.transaction_log
      tablesample bernoulli (1) ))"

echo "------- Update random rows from table... -------"
psql $POSTGRESQL_CONN  -c \
  "update transaction_db_raw.transaction_log
  set transaction_amount = round(random()::numeric, 2)
  where ctid = any(array(
    select ctid
    from transaction_db_raw.transaction_log
    tablesample bernoulli (1) ))"

echo "------- END! -------"
