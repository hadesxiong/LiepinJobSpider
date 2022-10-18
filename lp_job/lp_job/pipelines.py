# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from lp_job.items import JobItem,CmpItem,RecItem
from scrapy.exporters import CsvItemExporter

import pandas as pd
import pymysql,time,redis,configparser,json,sys,os

print(os.getcwd())

# 添加路径以便引入自定义组件
sys.path.append('../')

# configparser实例
cp = configparser.ConfigParser()

# 读取配置文件-db
cp.read('../configs/db.cfg')

mysql_config = {
    'host': cp.get('mysql_cfg', 'host'),
    'port': int(cp.get('mysql_cfg', 'port')),
    'user': cp.get('mysql_cfg', 'user'),
    'password': cp.get('mysql_cfg', 'password'),
    'database': cp.get('mysql_cfg', 'database')
}

redis_config = {
    'host': cp.get('redis_cfg', 'host'),
    'port': int(cp.get('redis_cfg', 'port')),
    'user': '',
    'password': cp.get('redis_cfg', 'password'),
    'db': int(cp.get('redis_cfg', 'db'))
}

class JobCardPipeline:

    def open_spider(self,spider):

        # 判断存储标记
        self.file_mark = spider.file_mark
        print(self.file_mark)
        if self.file_mark:
            self.result_list = []

        # mysql数据库
        # self.mysql_ins = pymysql.connect(
        #     host='10.162.165.211',
        #     port=1004,
        #     user='root',
        #     password='Faurecia614',
        #     database='lpjob_db'
        # )
        # self.mysql_cursor = self.mysql_ins.cursor()
        
        # redis 数据库
        self.redis_pool = redis.ConnectionPool(host=redis_config['host'], port=redis_config['port'],
                                  db=redis_config['db'], password=redis_config['password'], decode_responses=True)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)
        pass

    def process_item(self, item, spider):

        # 岗位信息

        job_item = JobItem()
        job_data = item.get('job')

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

        # 猎头/HR信息

        rec_item = RecItem()
        rec_data = item.get('recruiter')

        rec_item['rec_id'] = rec_data.get('recruiterId')
        rec_item['rec_name'] = rec_data.get('recruiterName')
        rec_item['rec_title'] = rec_data.get('recruiterTitle')
        rec_item['rec_imtype'] = rec_data.get('imUserType')
        rec_item['rec_imid'] = rec_data.get('imId')
        rec_item['rec_img'] = f'https://image0.lietou-static.com/big/{rec_data.get("recruiterPhoto")}'

        # 公司信息

        cmp_item = CmpItem()
        cmp_data = item.get('comp')
        cmp_item['cmp_id'] = cmp_data.get('compId')

        # cmp_id处理
        if cmp_item['cmp_id'] is not None:
            pass
        else:
            cmp_item['cmp_id']= str(int(time.time()))

        cmp_item['cmp_stage'] = cmp_data.get('compStage')
        cmp_item['cmp_logo'] = f'https://image0.lietou-static.com/big/{cmp_data.get("compLogo")}'
        cmp_item['cmp_name'] = cmp_data.get('compName')
        cmp_item['cmp_scale'] = cmp_data.get('compScale')
        cmp_item['cmp_industry'] = cmp_data.get('compIndustry')
        cmp_item['cmp_link'] = cmp_data.get('link')

        # 补充job_item
        job_item['cmp_id'] = cmp_item['cmp_id']
        job_item['rec_id'] = rec_item['rec_id']
        
        # 导出数据
        output_item = {}
        output_item.update(job_item)
        output_item.update(cmp_item)
        output_item.update(rec_item)
        
        if self.file_mark:
            self.result_list.append(output_item)

        '''

        # mysql写入

        # cmp信息

        # cmp_id查询
        if cmp_item['cmp_id'] is not None:
            cmp_id = str(cmp_item['cmp_id'])
        else:
            cmp_id = str(int(time.time()))
        cmp_fsql = f'SELECT CMP_NAME FROM COMPANY WHERE CMP_ID = "{cmp_id}"'
        self.mysql_cursor.execute(cmp_fsql)
        cmp_fres = self.mysql_cursor.fetchall()

        # cmp_scale处理
        cmp_scale = cmp_item['cmp_scale']
        if "-" in cmp_scale and cmp_scale is not None:
            cmp_scale_list = cmp_scale.split("-")
            cmp_scale_list[1] = cmp_scale_list[1].strip('人')
        elif "人以上" in cmp_scale and cmp_scale is not None:
            cmp_scale_list = cmp_scale.split("人以上")
            cmp_scale_list[1] = 99999
        else:
            cmp_scale_list = [0,0]
        cmp_scale_list = list(map(lambda x: int(x),cmp_scale_list))

        # cmp_stage处理
        if cmp_item['cmp_stage'] is not None:
            pass
        else:
            cmp_item['cmp_stage'] = '融资未公开'

        # cmp写入
        if len(cmp_fres) == 0:
            print('new cmp obj.')
            cmp_table = '`CMP_ID`,`CMP_NAME`,`CMP_SCALE_MIN`,`CMP_SCALE_MAX`,`CMP_LINK`,`CMP_STAGE`,`CMP_LOGO`,`CMP_INDUSTRY`'
            cmp_value = '"{0}","{1}",{2},{3},"{4}","{5}","{6}","{7}"'.format(cmp_id,cmp_item['cmp_name'],cmp_scale_list[0],cmp_scale_list[1],cmp_item['cmp_link'],cmp_item['cmp_stage'],cmp_item['cmp_logo'],cmp_item['cmp_industry'])
            cmp_wsql = f"INSERT INTO `COMPANY` ({cmp_table}) VALUES ({cmp_value})"
            self.mysql_cursor.execute(cmp_wsql)
            self.mysql_ins.commit()

        # rec信息

        # rec_id查询
        rec_id = rec_item['rec_id']
        rec_fsql = f'SELECT REC_NAME FROM RECRUITER WHERE REC_ID = "{rec_id}"'
        rec_fres = self.mysql_cursor.execute(rec_fsql)

        # rec写入
        if rec_fres == 0:
            print('new rec obj.')
            rec_table = '`REC_ID`,`IM_ID`,`IM_USERTYPE`,`REC_NAME`,`REC_TITLE`,`REC_PIC`'
            rec_value = '"{0}","{1}","{2}","{3}","{4}","{5}"'.format(rec_id,rec_item['rec_imid'],rec_item['rec_imtype'],rec_item['rec_name'],rec_item['rec_title'],rec_item['rec_img'])
            rec_wsql = f"INSERT INTO `RECRUITER` ({rec_table}) VALUES ({rec_value})"
            self.mysql_cursor.execute(rec_wsql)
            self.mysql_ins.commit()

        # jobcard信息

        # jobcard_id查询
        jobcard_id = job_item['job_id']
        jobcard_fsql = f'SELECT JOB_NAME FROM JOBCARD WHERE JOBCARD_ID = "{jobcard_id}"'
        jobcard_fres = self.mysql_cursor.execute(jobcard_fsql)

        # job_salary处理
        # 25-40k·15薪
        # 薪资面议
        # 30-40k
        job_salary = job_item['job_salary']
        if '-' in job_salary and '·' in job_salary:
            job_salary_list = job_salary[:-4].strip('k').split('-')
            job_salary_count = int(job_salary[-3:-1])
        elif '-' in job_salary and '·' not in job_salary:
            job_salary_list = job_salary.strip('k').split('-')
            job_salary_count = 12
        else:
            job_salary_list = [0,0]
            job_salary_count = 0
        job_salary_list = list(map(lambda x: int(x),job_salary_list))

        # job_exp处理
        # 3-5年
        # 经验不限
        # 10年以上
        job_exp = job_item['job_years']
        if '-' in job_exp:
            job_exp_list = job_exp.strip('年').split('-')
        elif '以上' in job_exp:
            job_exp_list = [job_exp.strip('年以上'),99]
        else:
            job_exp_list = [0,99]
        job_exp_list = list(map(lambda x:int(x),job_exp_list))

        # job_dq处理
        job_area = job_item['job_dq']
        if '-' in job_area:
            job_city = job_area.split('-')[0]
            job_dq = job_area.split('-')[1]
        else:
            job_city = job_area
            job_dq = '全市'

        job_update = job_item['job_refreshtime']
        job_update = f'{job_update[0:4]}-{job_update[4:6]}-{job_update[6:8]}'
        
        # jobcard写入
        if jobcard_fres == 0:
            print('new jobcard obj')
            jobcard_table = '`JOBCARD_ID`,`JOB_NAME`,`JOB_SALARY_MIN`,`JOB_SALARY_MAX`,`JOB_SALARY_COUNT`,`JOB_EDU`,`JOB_CITY`,`JOB_DQ`,`JOB_LINK`,`JOB_KIND`,`JOB_EXP_MIN`,`JOB_EXP_MAX`,`JOB_UPDATE`,`CMP_ID_ID`,`REC_ID_ID`'
            jobcard_value = '"{0}","{1}",{2},{3},{4},"{5}","{6}","{7}","{8}","{9}",{10},{11},"{12}","{13}","{14}"'.format(jobcard_id,job_item['job_title'],job_salary_list[0],job_salary_list[1],job_salary_count,job_item['job_edu'],job_city,job_dq,job_item['job_link'],job_item['job_kind'],job_exp_list[0],job_exp_list[1],job_update,cmp_id,rec_id)
            jobcard_wsql = f'INSERT INTO JOBCARD ({jobcard_table}) VALUES ({jobcard_value})'
            self.mysql_cursor.execute(jobcard_wsql)
            self.mysql_ins.commit()
        
        # joblabel处理
        job_label = job_item['job_label']
        joblabel_id_list = []
        if len(job_label) > 0:
            for each_label in job_label:
                joblabel_fsql = f'SELECT LABEL_ID FROM JOBLABEL WHERE LABEL_NAME = "{each_label}"'
                joblabel_fres = self.mysql_cursor.execute(joblabel_fsql)
                if joblabel_fres == 0:
                    joblabel_wsql = f'INSERT INTO JOBLABEL (`LABEL_NAME`) VALUES ("{each_label}")'
                    self.mysql_cursor.execute(joblabel_wsql)
                    self.mysql_ins.commit()
                    self.mysql_cursor.execute(joblabel_fsql)
                    
                    

                each_id = self.mysql_cursor.fetchone()[0]
                joblabel_id_list.append(each_id)

        # 遍历joblabel_id_list写入cardlabel
        for each_id in joblabel_id_list:
            cardlabel_fsql = f'SELECT * FROM CARDLABEL WHERE LABEL_ID_ID = {each_id} AND JOBCARD_ID_ID = {jobcard_id}'
            cardlabel_fres = self.mysql_cursor.execute(cardlabel_fsql)
            if cardlabel_fres == 0:
                cardlabel_wsql = f'INSERT INTO CARDLABEL (`LABEL_ID_ID`,`JOBCARD_ID_ID`) VALUES ("{each_id}","{jobcard_id}")'
                self.mysql_cursor.execute(cardlabel_wsql)
                self.mysql_ins.commit()

        return ({'jobcard':job_item,'cmp':cmp_item,'rec':rec_item})

        '''

        # redis写入

        # cmp信息

        # cmp_stage处理
        if cmp_item['cmp_stage'] is not None:
            pass
        else:
            cmp_item['cmp_stage'] = '融资未公开'

        # cmp 写入cmp_list
        cmplist_res = self.redis_db.hsetnx('cmp_list',cmp_item['cmp_id'],json.dumps(dict(cmp_item)))

        # rec信息
        # rec写入rec_list
        rec_res = self.redis_db.hsetnx('rec_list',rec_item['rec_id'],json.dumps(dict(rec_item)))

        # jobcard信息
        # jobcard写入jc_list
        jc_res = self.redis_db.hsetnx('jc_list',job_item['job_id'],json.dumps(dict(job_item)))

        # 写入jobid_list
        jobid_list_res = self.redis_db.hsetnx('joblink_list',job_item['job_id'],json.dumps({'link':job_item['job_link'],'update':job_item['job_refreshtime']}))

        # joblabel信息
        jl_res = self.redis_db.hsetnx('jl_list',job_item['job_id'],','.join(job_item['job_label']))

        # jobcmp信息
        jc_res = self.redis_db.hsetnx('job_cmp',job_item['job_id'],cmp_item['cmp_id'])

        return item

    def close_spider(self,spider):

        if self.file_mark:
            self.df = pd.DataFrame(self.result_list)
            print(self.df)
            self.df.to_csv('./dataframe_result.csv')

        self.mysql_ins.close()
        pass