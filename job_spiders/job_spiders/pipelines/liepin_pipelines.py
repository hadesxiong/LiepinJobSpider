# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

# coding=utf8

from job_spiders.items.liepin_items import LiePinJobItem, LiePinCmpItem, LiePinRecItem
from scrapy.utils.project import get_project_settings

import pandas as pd
import time,redis,json,uuid

# 读取设置
settings  = get_project_settings()

class JobCardPipeline:

    def open_spider(self, spider):

        # 判断存储标记
        self.file_mark = spider.file_mark
        print(self.file_mark)
        if self.file_mark:
            self.result_list = []

        # REDIS数据库
        self.redis_pool = redis.ConnectionPool(
            host=settings.get('REDIS_HOST'),
            port=settings.get('REDIS_PORT'),
            db=settings.get('REDIS_DB_STORGE'),
            password=settings.get('REDIS_PASSWORD'),
            decode_responses=True)
        self.redis_db =  redis.Redis(connection_pool=self.redis_pool)

        # 声明数据存储列表
        self.cmp_list,self.rec_list,self.jc_list,self.label_list,self.jl_list = [],[],[],[],[]

        pass

    def process_item(self, item, spider):

        data_item = item.get('item')
        key_word = item.get('key_word')

        # 岗位信息

        job_item = LiePinJobItem()
        job_data = data_item.get('job')

        job_item['job_id'] = job_data.get('jobId')
        job_item['job_label'] = job_data.get('labels')

        # label处理
        if job_item['job_label'] == '' or job_item['job_label'] is None:
            job_item['job_label'] = "无"

        job_item['job_refreshtime'] = job_data.get('refreshTime')
        job_item['job_salary'] = job_data.get('salary')
        job_item['job_edu'] = job_data.get('requireEduLevel')
        job_item['job_dq'] = job_data.get('dq')
        job_item['job_link'] = job_data.get('link')
        job_item['job_name'] = job_data.get('title')
        job_item['job_years'] = job_data.get('requireWorkYears')
        job_item['job_kind'] = job_data.get('jobKind')
        job_item['job_refer'] = key_word 

        # 猎头/HR信息

        rec_item = LiePinRecItem()
        rec_data = data_item.get('recruiter')

        rec_item['rec_id'] = rec_data.get('recruiterId')
        rec_item['rec_name'] = rec_data.get('recruiterName')
        rec_item['rec_title'] = rec_data.get('recruiterTitle')
        rec_item['rec_imtype'] = rec_data.get('imUserType')
        rec_item['rec_imid'] = rec_data.get('imId')
        rec_item['rec_img'] = f'https://image0.lietou-static.com/big/{rec_data.get("recruiterPhoto")}'

        # 公司信息

        cmp_item = LiePinCmpItem()
        cmp_data = data_item.get('comp')
        cmp_item['cmp_id'] = cmp_data.get('compId')

        # cmp_id处理
        if cmp_item['cmp_id'] is not None:
            pass
        else:
            cmp_item['cmp_id'] = str(int(time.time()))

        cmp_item['cmp_stage'] = cmp_data.get('compStage')
        cmp_item['cmp_logo'] = f'https://image0.lietou-static.com/big/{cmp_data.get("compLogo")}'
        cmp_item['cmp_name'] = cmp_data.get('compName')
        cmp_item['cmp_scale'] = cmp_data.get('compScale')
        cmp_item['cmp_industry'] = cmp_data.get('compIndustry')
        cmp_item['cmp_link'] = cmp_data.get('link')

        # 补充job_item
        job_item['cmp_id'] = cmp_item['cmp_id']
        job_item['rec_id'] = rec_item['rec_id']

        # 导出数据部分
        output_item = {}
        output_item.update(job_item)
        output_item.update(cmp_item)
        output_item.update(rec_item)

        if self.file_mark:
            self.result_list.append(output_item)

        # redis写入

        # cmp信息

        # cmp_stage处理
        if cmp_item['cmp_stage'] is not None:
            pass
        else:
            cmp_item['cmp_stage'] = '融资未公开'

        # cmp 写入cmp_list
        cmplist_res = self.redis_db.hsetnx('cmp_list', cmp_item['cmp_id'],json.dumps(dict(cmp_item)))

        # 根据写入结果判断是否要写入mysql
        if cmplist_res == 1:

            # cmp数据处理-cmp_scale
            cmp_scale = cmp_item['cmp_scale']
            if "-" in cmp_scale and cmp_scale is not None:
                cmp_scale_list = cmp_scale.split("-")
                cmp_scale_list[1] = cmp_scale_list[1].strip('人')
            elif "人以上" in cmp_scale and cmp_scale is not None:
                cmp_scale_list = cmp_scale.split("人以上")
                cmp_scale_list[1] = 99999
            else:
                cmp_scale_list = [0, 0]
            cmp_scale_list = list(map(lambda x: int(x), cmp_scale_list))

            # cmp数据处理-cmp_stage
            if cmp_item['cmp_stage'] is not None:
                pass
            else:
                cmp_item['cmp_stage'] = '融资未公开'

            # cmp_sql写入语句对照,字段顺序
            # CMP_ID,CMP_NAME,CMP_SCALE_MIN,CMP_SCALE_MAX,CMP_LINK,CMP_STAGE,CMP_LOGO,CMP_INDUSTRY
            cmp_value = (cmp_item['cmp_id'],cmp_item['cmp_name'],cmp_scale_list[0],cmp_scale_list[1],cmp_item['cmp_link'],cmp_item['cmp_stage'],cmp_item['cmp_logo'],cmp_item['cmp_industry'])

            # 组合
            self.cmp_list.append(cmp_value)

        else:
            pass

        # rec信息
        # rec写入rec_list
        rec_res = self.redis_db.hsetnx('rec_list', rec_item['rec_id'],json.dumps(dict(rec_item)))

        # 根据写入结果判断是否要写入mysql
        if rec_res == 1:

            # rec_sql写入语句对照,字段顺序
            # REC_ID,IM_ID,IM_USERTYPE,REC_NAME,REC_TITLE,REC_PIC
            rec_value = (rec_item['rec_id'],rec_item['rec_imid'],rec_item['rec_imtype'],rec_item['rec_name'],rec_item['rec_title'],rec_item['rec_img'])

            # 组合
            self.rec_list.append(rec_value)

        else:
            pass


        # jobcard信息
        # jobcard写入jc_list
        jc_res = self.redis_db.hsetnx('jc_list', job_item['job_id'],json.dumps(dict(job_item)))

        # 根据写入结果判断是否要写入mysql
        if jc_res == 1:

            # jc数据处理-job_salary
            # 可能的情况:25-40k·15薪,30-40k,薪资面议,100元/日,100元/天
            job_salary = job_item['job_salary']
            if '/天' in job_salary or '/日' in job_salary:
                job_salary_list = [0, 0]
                job_salary_count = 0
            elif '-' in job_salary and '·' in job_salary:
                job_salary_list = job_salary[:-4].strip('k').split('-')
                job_salary_count = int(job_salary[-3:-1])
            elif '-' in job_salary and '·' not in job_salary:
                job_salary_list = job_salary.strip('k').split('-')
                job_salary_count = 12
            else:
                job_salary_list = [0, 0]
                job_salary_count = 0
            job_salary_list = list(map(lambda x: int(x), job_salary_list))

            # jc数据处理-job_exp
            # 可能的情况:3-5年,经验不限,10年以上
            job_exp = job_item['job_years']
            if job_exp is None:
                job_exp_list = [0, 99]
            elif '-' in job_exp:
                job_exp_list = job_exp.strip('年').split('-')
            elif '以上' in job_exp:
                job_exp_list = [job_exp.strip('年以上'), 99]
            else:
                job_exp_list = [0, 99]
            job_exp_list = list(map(lambda x: int(x), job_exp_list))

            # jc数据处理-job_dq处理
            job_area = job_item['job_dq']
            if '-' in job_area:
                job_city = job_area.split('-')[0]
                job_dq = job_area.split('-')[1]
            else:
                job_city = job_area
                job_dq = '全市'

            # jc数据处理-job_update
            job_update = job_item['job_refreshtime']
            job_update = f'{job_update[0:4]}-{job_update[4:6]}-{job_update[6:8]}'

            # jc_sql写入语句对照,字段顺序
            # JOBCARD_ID,JOB_NAME,JOB_SALARY_MIN,JOB_SALARY_MAX,JOB_SALARY_COUNT,JOB_EDU,JOB_CITY,JOB_DQ,JOB_LINK,JOB_KIND,JOB_EXP_MIN,JOB_EXP_MAX,JOB_UPDATE,CMP_ID_ID,REC_ID_ID,JOB_REFER
            jc_value = (job_item['job_id'],job_item['job_name'],job_salary_list[0],job_salary_list[1],job_salary_count,job_item['job_edu'],job_city,job_dq,job_item['job_link'],job_item['job_kind'],job_exp_list[0],job_exp_list[1],job_update,job_item['cmp_id'],job_item['rec_id'],job_item['job_refer'])

            # 组合
            self.jc_list.append(jc_value)

        else:
            pass

        # label信息
        # 创建一个idlist用来保留label_id,遍历job_label
        lbid_list = []
        for each_label in job_item['job_label']:
            label_id = str(uuid.uuid4())
            label_res = self.redis_db.hsetnx('label_list',each_label,label_id)

            # 根据写入结果判断是否要写入mysql
            if label_res == 1:

                # label_sql写入语句对照,字段顺序
                # LABEL_ID,LABEL_NAME
                label_value = (label_id,each_label)

                # 组合
                self.label_list.append(label_value)
            
            else:
                # 获取现有的label_id进行覆盖
                label_id = self.redis_db.hget('label_list',each_label)

            lbid_list.append(label_id)

        # joblabel信息
        jl_res = self.redis_db.hsetnx('jl_list', job_item['job_id'],','.join(lbid_list))

        # 根据写入结果判断是否要写入mysql
        if jl_res == 1:

            # jl_sql写入语句对照,字段顺序
            # JOBCARD_ID_ID,LABEL_ID_ID
            for each_id in lbid_list:
                jl_value = (job_item['job_id'],each_id)
                self.jl_list.append(jl_value)

        else:
            pass

        return item
    
    def close_spider(self,spider):

        pass