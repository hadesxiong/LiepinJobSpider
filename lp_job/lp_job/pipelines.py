# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from lp_job.items import JobItem, CmpItem, RecItem
from scrapy.exporters import CsvItemExporter
from scrapy.utils.project import get_project_settings

import pandas as pd
import pymysql, time, redis, json, sys, os

print(os.getcwd())

# 添加路径以便引入自定义组件
sys.path.append('../')

# configparser实例
# cp = configparser.ConfigParser()

# 读取配置文件-db
# cp.read('../configs/db.cfg')

# mysql_config = {
#     'host': cp.get('mysql_cfg', 'host'),
#     'port': int(cp.get('mysql_cfg', 'port')),
#     'user': cp.get('mysql_cfg', 'user'),
#     'password': cp.get('mysql_cfg', 'password'),
#     'database': cp.get('mysql_cfg', 'database')
# }

# 读取设置
settings = get_project_settings()

class JobCardPipeline:

    def open_spider(self, spider):

        # 判断存储标记
        self.file_mark = spider.file_mark
        print(self.file_mark)
        if self.file_mark:
            self.result_list = []

        # redis 数据库
        self.redis_pool = redis.ConnectionPool(
            host=settings.get('REDIS_HOST'),
            port=settings.get('REDIS_PORT'),
            db=settings.get('REDIS_DB_STORGE'),
            password=settings.get('REDIS_PASSWORD'),
            decode_responses=True)
        self.redis_db =  redis.Redis(connection_pool=self.redis_pool)
        pass

    def process_item(self, item, spider):

        data_item = item.get('item')
        key_word = item.get('key_word')

        # 岗位信息

        job_item = JobItem()
        job_data = data_item.get('job')

        job_item['job_id'] = job_data.get('jobId')
        job_item['job_label'] = job_data.get('labels')

        # label处理
        if job_item['job_label'] == '':
            job_item['job_label'] = "无"

        job_item['job_refreshtime'] = job_data.get('refreshTime')
        job_item['job_salary'] = job_data.get('salary')
        job_item['job_edu'] = job_data.get('requireEduLevel')
        job_item['job_dq'] = job_data.get('dq')
        job_item['job_link'] = job_data.get('link')
        job_item['job_title'] = job_data.get('title')
        job_item['job_years'] = job_data.get('requireWorkYears')
        job_item['job_kind'] = job_data.get('jobKind')
        job_item['job_refer'] = key_word 

        # 猎头/HR信息

        rec_item = RecItem()
        rec_data = data_item.get('recruiter')

        rec_item['rec_id'] = rec_data.get('recruiterId')
        rec_item['rec_name'] = rec_data.get('recruiterName')
        rec_item['rec_title'] = rec_data.get('recruiterTitle')
        rec_item['rec_imtype'] = rec_data.get('imUserType')
        rec_item['rec_imid'] = rec_data.get('imId')
        rec_item['rec_img'] = f'https://image0.lietou-static.com/big/{rec_data.get("recruiterPhoto")}'

        # 公司信息

        cmp_item = CmpItem()
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




        # rec信息
        # rec写入rec_list
        rec_res = self.redis_db.hsetnx('rec_list', rec_item['rec_id'],json.dumps(dict(rec_item)))

        # jobcard信息
        # jobcard写入jc_list
        jc_res = self.redis_db.hsetnx('jc_list', job_item['job_id'],json.dumps(dict(job_item)))

        # 写入jobid_list
        jobid_list_res = self.redis_db.hsetnx('joblink_list', job_item['job_id'],json.dumps({'link': job_item['job_link'],'update': job_item['job_refreshtime']}))

        # joblabel信息
        jl_res = self.redis_db.hsetnx('jl_list', job_item['job_id'],','.join(job_item['job_label']))

        return item

    def close_spider(self, spider):

        # 文件导出部分
        if self.file_mark:
            self.df = pd.DataFrame(self.result_list)
            print(self.df)
            self.df.to_csv('./dataframe_result.csv')

        # mysql写入部分
        # cmp写入
        cmp_sql = "INSERT INTO "

        pass