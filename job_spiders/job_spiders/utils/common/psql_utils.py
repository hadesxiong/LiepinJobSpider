# coding=utf8
# run alone, called by os

import psycopg2

def psql_fetch(cursor,batch_size=1000):

    fetch_result = []
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            return fetch_result
        for row in rows:
            fetch_result.append(row[0])