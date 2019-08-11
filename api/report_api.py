#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
import flask
import json

app = flask.Flask(__name__)

params = {"host":"172.17.0.2", "port":"5432",
    "database":"rn0z", "user":"rn0z", "password":"1zx2"}

def db_conn():
    return psycopg2.connect(**params)#'postgres://rn0z:1zx2@172.17.0.2:5432/rn0z')


def to_json(data):
    return json.dumps(data) + "\n"


def resp(code, data):
    return flask.Response(
        status=code,
        mimetype="application/json",
        response=to_json(data)
    )

def affected_num_to_code(cnt):
    code = 200
    if cnt == 0:
        code = 404
    return code


@app.route('/')
def root():
    return flask.redirect('/api/1.0/transactions_daily')

# e.g. failed to parse json
@app.errorhandler(400)
def page_not_found(e):
    return resp(400, {})


@app.errorhandler(404)
def page_not_found(e):
    return resp(400, {})


@app.errorhandler(405)
def page_not_found(e):
    return resp(405, {})


@app.route('/api/1.0/transactions_by_day', methods=['GET'])
def get_transactions_by_day():
    with db_conn() as db:
        cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
                    select
                    	t.account_uid,
                    	to_char(t.transaction_date, 'YYYY-MM-DD') as date_oper,
                    	sum(t.transaction_amount) as sum_tran
                    from transaction_db_clean.transaction_log t
                    group by
                    	t.account_uid,
                    	to_char(t.transaction_date, 'YYYY-MM-DD')
                    order by 1, 2""")
        tuples = cur.fetchall()

        transactions = []
        for (account_uid, date_oper, sum_tran) in tuples:
            transactions.append({"account_uby_dayid": account_uid,
                "date_oper": date_oper,
                "sum_tran": sum_tran})
        return resp(200, {"transactions_by_day": transactions})


@app.route('/api/1.0/transactions_by_month', methods=['GET'])
def get_transactions_by_month():
    with db_conn() as db:
        cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
                select
                	t.account_uid,
                	date_part('year', t.transaction_date) as year_oper,
                	date_part('month', t.transaction_date) as month_oper,
                	sum(t.transaction_amount) as sum_tran
                from transaction_db_clean.transaction_log t
                group by
                	t.account_uid,
                	date_part('year', t.transaction_date),
                	date_part('month', t.transaction_date)
                order by 1, 2, 3""")
        tuples = cur.fetchall()

        transactions = []
        for (account_uid, year_oper, month_oper, sum_tran) in tuples:
            transactions.append({"account_uid": account_uid,
                "year_oper": year_oper,
                "month_oper": month_oper,
                "sum_tran": sum_tran})
        return resp(200, {"transactions_by_month": transactions})


@app.route('/api/1.0/transactions_by_account', methods=['GET'])
def get_transactions_by_account():
    with db_conn() as db:
        cur = db.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute("""
                select
                	t.account_uid,
                	sum(t.transaction_amount) as sum_tran
                from transaction_db_clean.transaction_log t
                group by
                	t.account_uid
                order by 1""")
        tuples = cur.fetchall()

        transactions = []
        for (account_uid, sum_tran) in tuples:
            transactions.append({"account_uid": account_uid,
                "sum_tran": sum_tran})
        return resp(200, {"transactions_by_account": transactions})

if __name__ == '__main__':
    app.debug = True  # enables auto reload during development
    app.run()
