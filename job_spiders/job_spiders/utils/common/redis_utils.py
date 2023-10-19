# coding=utf8
# redis common usage

import redis,json

def read_hash(redis_conn,hash_key,batch_size=1000):

    cursor = 0
    read_result = []
    while True:
        cursor,data = redis_conn.hscan(hash_key,cursor,count=batch_size)
        # 处理读取到的数据
        for key,value in data.items():
            # print(f"Key: {key}, Value: {json.loads(value)}")
            read_result.append((key,json.loads(value)))
        if cursor == 0:
            break

    return read_result