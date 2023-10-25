# coding=utf8
# run alone, called by os

import psycopg2,json

import sys,os
# 解决相对路径引用
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from utils_conf.database_conf import postgresql_conf as psql_conf
from common.psql_utils import psql_fetch

# 配置psql
# 配置psql
psql_conn = psycopg2.connect(
    host=psql_conf['host'],
    port=psql_conf['port'],
    database=psql_conf['db'],
    user=psql_conf['user'],
    password=psql_conf['pw']
)

psql_cursor = psql_conn.cursor()
psql_cursor.execute('SET search_path TO job_spiders')

# 查询现有的key
psql_cursor.execute('SELECT JOB_ID FROM JOB_CARD')
jckey_exists = psql_fetch(psql_cursor,1000)

print(jckey_exists)

# 关闭psql
psql_cursor.close()
psql_conn.close()