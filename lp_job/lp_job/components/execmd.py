import subprocess


def crawl_jc(redis_db, logger_obj):
    # 遍历jobcard_list清单
    jclist_check = redis_db.exists('lpjc_cmdlist')

    if jclist_check == 0:
        logger_obj.info('lpjc_cmdlist has no cmd to be crawled.')

    else:
        jobcard_id= list(redis_db.hgetall('lpjc_cmdlist').keys())[0]
        jobcard_cmd = list(redis_db.hgetall('lpjc_cmdlist').values())[0]
        logger_obj.info(f'jobcard_cmd: {jobcard_cmd}')
        cmd_res = subprocess.Popen(jobcard_cmd, shell=True)
        cmd_code = cmd_res.wait()
        logger_obj.info(f'jobcard_cmd result : {cmd_code}')
        redis_db.hdel('lpjc_cmdlist', jobcard_id)

        if cmd_code != 0:
            logger_obj.info(
                f'jobcard_cmd {jobcard_cmd} errors, it has been pushed into lpjc_cmderror.')
            redis_db.hsetnx('lpjc_cmderror', jobcard_id, jobcard_cmd)
        else:
            logger_obj.info(f'jobcard_cmd {jobcard_cmd} finish executed.')