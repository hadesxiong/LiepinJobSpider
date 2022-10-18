import time, json, pymysql


def cmp_etl(mysql_config, redis_db, logger_obj):

    # mysql_ins配置
    mysql_ins = pymysql.connect(host=mysql_config['host'],
                                port=mysql_config['port'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database'])
    mysql_cursor = mysql_ins.cursor()

    # 获取cmp_list/cmp_histroy-爬取/历史
    cmp_list = redis_db.hgetall('cmp_list')
    cmp_histroy = redis_db.hgetall('cmp_histroy')
    cmpid_new = list(set(cmp_list.keys()).difference(set(cmp_histroy.keys())))
    if len(cmpid_new) != 0:
        cmp_new = list(map(lambda x: {x: cmp_list.get(x)}, cmpid_new))

    else:
        cmp_new = []

    logger_obj.info(f'cmp_new length is {len(cmp_new)}')

    if len(cmp_new) != 0:
        for each_cmp in cmp_new:
            cmp_id = list(each_cmp.keys())[0]
            cmp_data = json.loads(list(each_cmp.values())[0])
            cmp_data['cmp_id'] = cmp_id

            cmp_fsql = f'SELECT CMP_NAME FROM COMPANY WHERE CMP_ID = "{cmp_id}"'
            mysql_cursor.execute(cmp_fsql)
            cmp_fres = mysql_cursor.fetchall()

            if len(cmp_fres) == 0:

                # 处理cmp_scale
                cmp_scale = cmp_data['cmp_scale']
                if "-" in cmp_scale and cmp_scale is not None:
                    cmp_scale_list = cmp_scale.split("-")
                    cmp_scale_list[1] = cmp_scale_list[1].strip('人')
                elif "人以上" in cmp_scale and cmp_scale is not None:
                    cmp_scale_list = cmp_scale.split("人以上")
                    cmp_scale_list[1] = 99999
                else:
                    cmp_scale_list = [0, 0]
                cmp_scale_list = list(map(lambda x: int(x), cmp_scale_list))

                # 处理cmp_stage
                if cmp_data['cmp_stage'] is not None:
                    pass
                else:
                    cmp_data['cmp_stage'] = '融资未公开'

                # cmp写入
                cmp_table = '`CMP_ID`,`CMP_NAME`,`CMP_SCALE_MIN`,`CMP_SCALE_MAX`,`CMP_LINK`,`CMP_STAGE`,`CMP_LOGO`,`CMP_INDUSTRY`'
                cmp_value = '"{0}","{1}",{2},{3},"{4}","{5}","{6}","{7}"'.format(
                    cmp_id, cmp_data['cmp_name'], cmp_scale_list[0],
                    cmp_scale_list[1], cmp_data['cmp_link'],
                    cmp_data['cmp_stage'], cmp_data['cmp_logo'],
                    cmp_data['cmp_industry'])
                cmp_wsql = f"INSERT INTO `COMPANY` ({cmp_table}) VALUES ({cmp_value})"
                mysql_cursor.execute(cmp_wsql)
                mysql_ins.commit()

                redis_db.hsetnx('cmp_history', cmp_id, str(int(time.time())))
                redis_db.hdel('cmp_list', cmp_id)

            else:
                pass


def rec_etl(mysql_config, redis_db, logger_obj):

    # mysql_ins配置
    mysql_ins = pymysql.connect(host=mysql_config['host'],
                                port=mysql_config['port'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database'])
    mysql_cursor = mysql_ins.cursor()

    # 获取rec_list/rec_history
    rec_list = redis_db.hgetall('rec_list')
    rec_history = redis_db.hgetall('rec_history')
    recid_new = list(set(rec_list.keys()).difference(set(rec_history.keys())))

    if len(recid_new) != 0:
        rec_new = list(map(lambda x: {x: rec_list.get(x)}, recid_new))
    else:
        rec_new = []

    logger_obj.info(f'rec_new length is {len(rec_new)}')

    if len(rec_new) != 0:
        for each_rec in rec_new:
            rec_id = list(each_rec.keys())[0]
            rec_data = json.loads(list(each_rec.values())[0])

            rec_fsql = f'SELECT REC_NAME FROM RECRUITER WHERE REC_ID = "{rec_id}"'
            rec_fres = mysql_cursor.execute(rec_fsql)

            if rec_fres == 0:
                rec_table = '`REC_ID`,`IM_ID`,`IM_USERTYPE`,`REC_NAME`,`REC_TITLE`,`REC_PIC`'
                rec_value = '"{0}","{1}","{2}","{3}","{4}","{5}"'.format(
                    rec_id, rec_data['rec_imid'], rec_data['rec_imtype'],
                    rec_data['rec_name'], rec_data['rec_title'],
                    rec_data['rec_img'])
                rec_wsql = f"INSERT INTO `RECRUITER` ({rec_table}) VALUES ({rec_value})"
                mysql_cursor.execute(rec_wsql)
                mysql_ins.commit()

                redis_db.hsetnx('rec_history', rec_id, str(int(time.time())))
                redis_db.hdel('rec_list', rec_id)

            else:
                pass


def jc_etl(mysql_config, redis_db, logger_obj):

    # mysql_ins配置
    mysql_ins = pymysql.connect(host=mysql_config['host'],
                                port=mysql_config['port'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database'])
    mysql_cursor = mysql_ins.cursor()

    # 获取jc_list/jc_history-爬取/历史
    jc_list = redis_db.hgetall('jc_list')
    jc_history = redis_db.hgetall('jc_history')
    jcid_new = list(set(jc_list.keys()).difference(set(jc_history.keys())))

    if len(jcid_new) != 0:
        jc_new = list(map(lambda x: {x: jc_list.get(x)}, jcid_new))
    else:
        jc_new = []

    if len(jc_new) != 0:
        for each_jc in jc_new:
            jc_id = list(each_jc.keys())[0]
            jc_data = json.loads(list(each_jc.values())[0])

            jc_fsql = f'SELECT JOB_NAME FROM JOBCARD WHERE JOBCARD_ID = "{jc_id}"'
            jc_fres = mysql_cursor.execute(jc_fsql)

            if jc_fres == 0:

                # job_salary处理
                # 25-40k·15薪
                # 薪资面议
                # 30-40k
                job_salary = jc_data['job_salary']
                if '-' in job_salary and '·' in job_salary:
                    job_salary_list = job_salary[:-4].strip('k').split('-')
                    job_salary_count = int(job_salary[-3:-1])
                elif '-' in job_salary and '·' not in job_salary:
                    job_salary_list = job_salary.strip('k').split('-')
                    job_salary_count = 12
                else:
                    job_salary_list = [0, 0]
                    job_salary_count = 0
                job_salary_list = list(map(lambda x: int(x), job_salary_list))

                # job_exp处理
                # 3-5年
                # 经验不限
                # 10年以上
                job_exp = jc_data['job_years']
                if '-' in job_exp:
                    job_exp_list = job_exp.strip('年').split('-')
                elif '以上' in job_exp:
                    job_exp_list = [job_exp.strip('年以上'), 99]
                else:
                    job_exp_list = [0, 99]
                job_exp_list = list(map(lambda x: int(x), job_exp_list))

                # job_dq处理
                job_area = jc_data['job_dq']
                if '-' in job_area:
                    job_city = job_area.split('-')[0]
                    job_dq = job_area.split('-')[1]
                else:
                    job_city = job_area
                    job_dq = '全市'

                job_update = jc_data['job_refreshtime']
                job_update = f'{job_update[0:4]}-{job_update[4:6]}-{job_update[6:8]}'

                # jc写入
                jc_table = '`JOBCARD_ID`,`JOB_NAME`,`JOB_SALARY_MIN`,`JOB_SALARY_MAX`,`JOB_SALARY_COUNT`,`JOB_EDU`,`JOB_CITY`,`JOB_DQ`,`JOB_LINK`,`JOB_KIND`,`JOB_EXP_MIN`,`JOB_EXP_MAX`,`JOB_UPDATE`,`CMP_ID_ID`,`REC_ID_ID`,`JOB_REFER`'
                jc_value = '"{0}","{1}",{2},{3},{4},"{5}","{6}","{7}","{8}","{9}",{10},{11},"{12}","{13}","{14}","{15}"'.format(
                    jc_id, jc_data['job_title'], job_salary_list[0],
                    job_salary_list[1], job_salary_count, jc_data['job_edu'],
                    job_city, job_dq, jc_data['job_link'], jc_data['job_kind'],
                    job_exp_list[0], job_exp_list[1], job_update,
                    jc_data['cmp_id'], jc_data['rec_id'],jc_data['job_refer'])
                jc_wsql = f'INSERT INTO JOBCARD ({jc_table}) VALUES ({jc_value})'

                try:
                    mysql_cursor.execute(jc_wsql)
                    mysql_ins.commit()

                    redis_db.hsetnx('jc_history', jc_id, str(int(time.time())))
                    redis_db.hdel('jc_list', jc_id)

                except:
                    pass

            else:
                pass


def jl_etl(mysql_config, redis_db, logger_obj):

    # mysql_ins配置
    mysql_ins = pymysql.connect(host=mysql_config['host'],
                                port=mysql_config['port'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database'])
    mysql_cursor = mysql_ins.cursor()

    jl_list = redis_db.hgetall('jl_list')

    etl_finish = False

    for each_jl in jl_list.items():
        job_id = int(each_jl[0])
        label_list = list(set(each_jl[1].split(',')))
        labelid_list = []

        for each_label in label_list:
            label_fsql = f'SELECT LABEL_ID FROM JOBLABEL WHERE LABEL_NAME = "{each_label}"'
            label_fres = mysql_cursor.execute(label_fsql)
            if label_fres == 0:
                label_wsql = f'INSERT INTO JOBLABEL (`LABEL_NAME`) VALUES ("{each_label}")'
                mysql_cursor.execute(label_wsql)
                mysql_ins.commit()
                mysql_cursor.execute(label_fsql)

            label_id = int(mysql_cursor.fetchone()[0])
            labelid_list.append(label_id)

        for each_labelid in labelid_list:
            joblabel_fsql = f'SELECT * FROM CARDLABEL WHERE LABEL_ID_ID = {each_labelid} AND JOBCARD_ID_ID = {job_id}'
            joblabel_fres = mysql_cursor.execute(joblabel_fsql)

            if joblabel_fres == 0:
                joblabel_wsql = f'INSERT INTO CARDLABEL (`LABEL_ID_ID`,`JOBCARD_ID_ID`) VALUES ({each_labelid},{job_id})'

                try:
                    mysql_cursor.execute(joblabel_wsql)
                    mysql_ins.commit()
                    if each_labelid == labelid_list[-1]:
                        etl_finish = True

                except:
                    pass

        if etl_finish:
            redis_db.hdel('jl_list', job_id)


def jd_etl(mysql_config, redis_db, logger_obj):

    # mysql_ins配置
    mysql_ins = pymysql.connect(host=mysql_config['host'],
                                port=mysql_config['port'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database'])
    mysql_cursor = mysql_ins.cursor()

    jd_list = redis_db.hgetall('jd_list')

    for each_jd in jd_list:

        job_id = list(each_jd.keys())[0]
        job_detail = list(each_jd.values())[0]
        job_detailid = f'JD_{str(job_id)}'

        jd_fsql = f'SELECT JOBDETAIL_ID FROM JOBDETAIL WHERE JOBDETAIL_ID = {job_detailid}'
        jd_fres = mysql_cursor.execute(jd_fsql)

        if jd_fres == 0:

            jd_table = '`JOBDETAIL_ID`,`JOBDETAIL`'
            jd_value = '"{0}","{1}"'.format(job_detailid, job_detail)
            jd_wsql = f"INSERT INTO `JOBDETAIL` ({jd_table}) VALUES ({jd_value})"
            mysql_cursor.execute(jd_wsql)
            mysql_ins.commit()

            redis_db.hdel('jd_list', job_id)

            jc_table = '`JOBDETAIL_ID_ID`'
            jc_value = '"{0}"'.format(job_detailid)
            jc_wsql = f"INSERT INTO `JOBCARD` ({jc_table}) VALUES ({jc_value}) WHERE `JOB_ID` = {job_id}"
            mysql_cursor.execute(jc_wsql)
            mysql_ins.commit()