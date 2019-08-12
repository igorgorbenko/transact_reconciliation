# transact_reconciliation

1. Database preparing via Docker (optional part, cause  you can use your
    own DB server and etc.)
    for example:
        -- docker run --name pg -d -e POSTGRES_USER=rn0z -e POSTGRES_PASSWORD=1zx2 postgres
        -- docker run -p 80:80 -e "PGADMIN_DEFAULT_EMAIL=user@domain.com" -e "PGADMIN_DEFAULT_PASSWORD=12345" -d dpage/pgadmin4

2. Put the database url in config file conf/db.ini

3. Run script for the test data preparation:
    ./test_data_creating.sh 10000

4. Run reconciliation script:
    python ./reconciliation_start.py

5. Run flask application:
    python api/report_api.py

    Reports:
        -- curl -XGET 'http://127.0.0.1:5000/api/1.0/transactions_by_account'
        -- curl -XGET 'http://127.0.0.1:5000/api/1.0/transactions_by_day'
        -- curl -XGET 'http://127.0.0.1:5000/api/1.0/transactions_by_month'
