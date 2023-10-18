# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

# coding=utf8

from job_spiders.items.liepin_items import LiePinJobItem
from scrapy.exceptions import DropItem

import redis,json,psycopg2

class JobCardPipeline:

    @classmethod
    def from_crawler(cls,crawler):
        # psql配置信息
        psql_conf = {
            'psql_host': crawler.settings.get('POSTGRESQL_HOST'),
            'psql_port': crawler.settings.get('POSTGRESQL_PORT'),
            'psql_db': crawler.settings.get('POSTGRESQL_DATABASE'),
            'psql_user': crawler.settings.get('POSTGRESQL_USER'),
            'psql_pw': crawler.settings.get('POSTGRESQL_PW'),
            'psql_schema': crawler.settings.get('POSTGRESQL_SCHEMA')
        }
        # redis配置信息
        redis_conf = {
            'redis_host': crawler.settings.get('REDIS_HOST'),
            'redis_port': crawler.settings.get('REDIS_PORT'),
            'redis_db': crawler.settings.get('REDIS_DB_STORGE'),
            'redis_pw': crawler.settings.get('REDIS_PASSWORD')
        }

        return cls(psql_conf,redis_conf)

    def __init__(self,psql_conf,redis_conf):

        self.psql_conf = psql_conf
        self.redis_conf = redis_conf

    def open_spider(self, spider):

        # 判断存储标记
        self.file_mark = spider.file_mark
        if self.file_mark:
            self.result_list = []

        # REDIS数据库
        self.redis_pool = redis.ConnectionPool(
            host=self.redis_conf.get('redis_host'),
            port=self.redis_conf.get('redis_port'),
            db=self.redis_conf.get('redis_db'),
            password=self.redis_conf.get('redis_pw'),
            decode_responses=True)
        self.redis_db =  redis.Redis(connection_pool=self.redis_pool)

        # psql数据库
        
        self.psql_con = psycopg2.connect(
            host=self.psql_conf.get('psql_host'),
            port=self.psql_conf.get('psql_port'),
            database=self.psql_conf.get('psql_db'),
            user=self.psql_conf.get('psql_user'),
            password=self.psql_conf.get('psql_pw')
        )

        # 声明数据存储列表
        self.jc_list = []

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
        job_item['job_label'] = job_data.get('labels')
        job_item['cmp_id'] = data_item.get('comp').get('compId')
        job_item['rec_id'] = data_item.get('recruiter').get('recruiterId')
        job_item['job_refer'] = key_word 

        # jobcard信息
        # jobcard写入jc_list
        jc_res = self.redis_db.hsetnx('job_spiders_liepin_jobcard', job_item['job_id'], json.dumps(dict(job_item)))

        # 根据写入结果判断是否要写入
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
            # JOBCARD_ID,JOB_NAME,JOB_SALARY_MIN,JOB_SALARY_MAX,
            # JOB_SALARY_COUNT,JOB_EDU,JOB_CITY,JOB_DQ,
            # JOB_LINK,JOB_KIND,JOB_EXP_MIN,JOB_EXP_MAX,
            # JOB_UPDATE,CMP_ID_ID,REC_ID_ID,JOB_REFER,JOB_LABEL
            jc_value = (
                job_item['job_id'],job_item['job_name'],job_salary_list[0],job_salary_list[1],
                job_salary_count,job_item['job_edu'],job_city,job_dq,
                job_item['job_link'],job_item['job_kind'],job_exp_list[0],job_exp_list[1],
                job_update,job_item['cmp_id'],job_item['rec_id'],job_item['job_refer'],job_item['job_label'])

            # 组合
            self.jc_list.append(jc_value)
            
            return item

        else:
            DropItem()
    
    def close_spider(self,spider):

        # print(self.psql_con)
        self.psql_cursor = self.psql_con.cursor()
        self.psql_cursor.execute('SET search_path TO job_spiders')

        psql_jobcard_query = 'INSERT INTO JOB_CARD \
            (JOBCARD_ID,JOB_NAME,JOB_SALARY_MIN,JOB_SALARY_MAX, \
            JOB_SALARY_COUNT,JOB_EDU,JOB_CITY,JOB_DQ,JOB_LINK, \
            JOB_KIND,JOB_EXP_MIN,JOB_EXP_MAX,JOB_UPDATE, \
            CMP_ID,REC_ID,JOB_REFER,JOB_LABEL) VALUES (  \
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    
        self.psql_cursor.executemany(psql_jobcard_query,self.jc_list)
        self.psql_con.commit()

        self.psql_cursor.close()
        self.psql_con.close()
        pass

