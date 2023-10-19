# coding=utf8

from job_spiders.items.liepin_items import LiePinCmpItem
from scrapy.exceptions import DropItem

import redis,json,time

class JobCmpPipeline:

    @classmethod
    def from_crawler(cls,crawler):

        # redis配置信息
        redis_conf = {
            'redis_host': crawler.settings.get('REDIS_HOST'),
            'redis_port': crawler.settings.get('REDIS_PORT'),
            'redis_db': crawler.settings.get('REDIS_DB_STORGE'),
            'redis_pw': crawler.settings.get('REDIS_PW')
        }

        return cls(redis_conf)
    
    def __init__(self,redis_conf):

        self.redis_conf = redis_conf

    def open_spider(self,spider):

        # REDIS数据库
        self.redis_pool = redis.ConnectionPool(
            host=self.redis_conf.get('redis_host'),
            port=self.redis_conf.get('redis_port'),
            db=self.redis_conf.get('redis_db'),
            password=self.redis_conf.get('redis_pw'),
            decode_responses=True)
        self.redis_db =  redis.Redis(connection_pool=self.redis_pool)
        
    def process_item(self,item,spider):

        if item is not None:

            data_item = item.get('item')

            cmp_item = LiePinCmpItem()
            cmp_data = data_item.get('comp')
            cmp_item['cmp_id'] = str(cmp_data.get('compId'))

            # cmp_id处理
            if cmp_item['cmp_id'] is not None:
                pass
            else:
                cmp_item['cmp_id'] = str(int(time.time()))


            cmp_item['cmp_stage'] = cmp_data.get('compStage')

            # cmp_stage处理
            if cmp_item['cmp_stage'] is not None:
                pass
            else:
                cmp_item['cmp_stage'] = '融资未公开'

            cmp_item['cmp_logo'] = f'https://image0.lietou-static.com/big/{cmp_data.get("compLogo")}'
            cmp_item['cmp_name'] = cmp_data.get('compName')
            cmp_item['cmp_scale'] = cmp_data.get('compScale')
            cmp_item['cmp_industry'] = cmp_data.get('compIndustry')
            cmp_item['cmp_link'] = cmp_data.get('link')

            # redis写入
            cmp_res = self.redis_db.hsetnx(
                'job_spiders_liepin_cmpinfo',
                cmp_item['cmp_id'],
                json.dumps(dict(cmp_item))
            )

            if cmp_res != 1:
                DropItem()
            else:
                return item
            
        else:
            pass
        
    def close_spider(self,spider):

        self.redis_pool.disconnect()