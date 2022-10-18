import itertools
import json


def setcmd_jc(redis_db, logger_obj, keyword_list, city_list):

    # 遍历迭代keyword+city
    word_city = list(itertools.product(keyword_list, city_list))
    cmd_list = list(map(lambda x: [
                    f"{x[0]}_{x[1]}", f"scrapy crawl lp_jobcard -a key_word={x[0]} -a city={x[1]}"], word_city))

    # 遍历插入列表
    for each_cmd in cmd_list:
        set_res = redis_db.hsetnx('lpjc_cmdlist', each_cmd[0], each_cmd[1])
        # 日志打印
        logger_obj.info(
            f'{each_cmd[0]}_{each_cmd[1]} insert result: {set_res}.')


def setcmd_jd(redis_db, logger_obj):

    # 检视redis
    jdlist_check = redis_db.exists('joblink_list')
    if jdlist_check == 0:
        logger_obj.info('jobid_list has no jobid to be crawled.')
    else:
        # joblist:hash,key:id,value:{UPDATETIME:updatetime,LINK:link}
        jobid_list = redis_db.hgetall('joblink_list')
        for each_obj in jobid_list.items():
            job_id = each_obj[0]
            job_link = json.dumps(each_obj[1]).get('link')
            set_res = redis_db.hsetnx('lpjd_cmdlist', job_id, f'scrapy crawl lp_jobdetail -a link={job_link} -a job_id={job_id}')
            logger_obj.info(
                f'jobid:{job_id},link:{job_link},insert result: {set_res}.')
            del_res = redis_db.hdel('joblink_list',job_id)
            logger_obj.info(
                f'jobid:{job_id},has been removed from joblink_list.'
            )