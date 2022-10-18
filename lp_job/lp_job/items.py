# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class JobItem(scrapy.Item):

    job_id = scrapy.Field() #id
    job_label = scrapy.Field() #标签，描述
    job_refreshtime = scrapy.Field() #更新时间
    job_salary = scrapy.Field() #薪资范围
    job_edu = scrapy.Field() #教育要求
    job_dq = scrapy.Field() #行政区域
    job_link = scrapy.Field() #岗位链接
    job_title = scrapy.Field() #岗位抬头
    job_years = scrapy.Field() #工作年限
    job_kind = scrapy.Field() #工作种类？待定
    rec_id = scrapy.Field() #猎头/hrid
    cmp_id = scrapy.Field() #公司id

class RecItem(scrapy.Item):

    rec_id = scrapy.Field() #id
    rec_name = scrapy.Field() #猎头名称
    rec_title = scrapy.Field() #猎头抬头
    rec_imtype = scrapy.Field() #imUserType
    rec_imid = scrapy.Field() #猎头头像id
    rec_img = scrapy.Field() #头像地址

class CmpItem(scrapy.Item):
    
    cmp_id = scrapy.Field() #id
    cmp_stage = scrapy.Field() #融资未公开
    cmp_logo = scrapy.Field() #公司logo
    cmp_name = scrapy.Field() #公司名称
    cmp_scale = scrapy.Field() #公司规模
    cmp_industry = scrapy.Field() #公司行业
    cmp_link = scrapy.Field() #公司链接