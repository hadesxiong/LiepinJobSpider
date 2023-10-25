# coding=utf8
# run alone, called by os

import psycopg2,redis,json

# 解决相对路径引用
import sys,os
sys.path.append(os.path.join(os.path.dirname(__file__),'..'))

from utils_conf.database_conf import postgresql_conf as psql_conf
from utils_conf.database_conf import redis_conf

from common.redis_utils import read_hash
from common.psql_utils import psql_fetch

# 配置redis
redis_pool = redis.ConnectionPool(
    host=redis_conf['host'],
    port=redis_conf['port'],
    db=redis_conf['storge'],
    password=redis_conf['pw'],
    decode_responses=True
)

redis_db = redis.Redis(connection_pool=redis_pool)

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

# 处理jobcard
jobcard_list = read_hash(redis_db,'job_spiders_liepin_jobcard',1000)

# 查询现有的key
psql_cursor.execute('SELECT JOB_ID FROM JOB_CARD')
jckey_exists = psql_fetch(psql_cursor,1000)

# jobcard过滤
jobcard_list_f = list(filter(lambda x: x[0] not in jckey_exists,jobcard_list))

# 处理recruiter_info
recinfo_list = read_hash(redis_db,'job_spiders_liepin_recinfo',1000)

# 查询现有的key
psql_cursor.execute('SELECT REC_ID FROM RECRUITER_INFO')
reckey_exists = psql_fetch(psql_cursor,1000)

# recruiter_info过滤
recinfo_list_f = list(filter(lambda x: x[0] not in reckey_exists,recinfo_list))

# 处理comp_info
cmpinfo_list = read_hash(redis_db,'job_spiders_liepin_cmpinfo',1000)

# 查询现有的key
psql_cursor.execute('SELECT CMP_ID FROM COMPANY_INFO')
cmpkey_exists = psql_fetch(psql_cursor,1000)

# cmpinfo过滤
cmpinfo_list_f = list(filter(lambda x: x[0] not in cmpkey_exists,cmpinfo_list))

# jobcard写入psql
if len(jobcard_list_f) != 0:
    jobcard_dict = [dict(row,job_resource='liepin') for row in list(map(lambda x: x[1], jobcard_list_f))]
    jobcard_columns = jobcard_dict[0].keys()
    jobcard_query = f"INSERT INTO JOB_CARD ({', '.join(jobcard_columns)}) VALUES ({', '.join(['%s']*len(jobcard_columns))})"

    psql_cursor.executemany(jobcard_query,[tuple(row[col] for col in jobcard_columns) for row in jobcard_dict])
    psql_conn.commit()

else:
    pass

# rec_info写入psql
if len(recinfo_list_f) != 0:
    recinfo_dict = [dict(row,rec_resource='liepin') for row in list(map(lambda x:x[1], recinfo_list_f))]
    recinfo_columns = recinfo_dict[0].keys()
    recinfo_query = f"INSERT INTO RECRUITER_INFO ({', '.join(recinfo_columns)}) VALUES ({', '.join(['%s']*len(recinfo_columns))})"

    psql_cursor.executemany(recinfo_query,[tuple(row[col] for col in recinfo_columns) for row in recinfo_dict])
    psql_conn.commit()

else:
    pass

if len(cmpinfo_list_f) != 0:
    cmpinfo_dict = [dict(row,cmp_resource='liepin') for row in list(map(lambda x:x[1], cmpinfo_list_f))]
    cmpinfo_columns = cmpinfo_dict[0].keys()
    cmpinfo_query = f"INSERT INTO COMPANY_INFO ({', '.join(cmpinfo_columns)}) VALUES ({', '.join(['%s']*len(cmpinfo_columns))})"

    psql_cursor.executemany(cmpinfo_query,[tuple(row[col] for col in cmpinfo_columns) for row in cmpinfo_dict])
    psql_conn.commit()

else:
    pass

# 关闭redis
redis_pool.disconnect()
# 关闭psql
psql_cursor.close()
psql_conn.close()