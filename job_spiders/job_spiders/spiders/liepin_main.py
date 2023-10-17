# coding=utf8
import scrapy,json

from job_spiders.config.liepin_conf import request_body as lp_body
from job_spiders.config.liepin_conf import request_headers as lp_headers

class jobItemSpider(scrapy.Spider):

    name = 'liepin_jobcard'

    def __init__(self,key_word,city, file_mark=False):

        self.url = "https://apic.liepin.com/api/com.liepin.searchfront4c.pc-search-job"
        self.file_mark = file_mark
        self.key_word = key_word
        self.city = city

        self.request_body = lp_body
        lp_body['data']['mainSearchPcConditionForm']['city'] = str(self.city)
        lp_body['data']['mainSearchPcConditionForm']['dq'] = str(self.city)
        lp_body['data']['mainSearchPcConditionForm']['key'] = self.key_word

        self.request_headers = lp_headers

    def start_requests(self):

        yield scrapy.Request(url=self.url,
                             method='POST',
                             body=json.dumps(self.request_body),
                             headers=self.request_headers,
                             callback=self.basic_parse)
        pass

    def basic_parse(self,response):

        r_data = json.loads(response.text)
        main_data = r_data.get('data').get('data')
        page_data = r_data.get('data').get('pagination')
        jobcard_data = main_data.get('jobCardList')

        for each_job in jobcard_data:
            # print(each_job)
            yield ({'item':each_job,'key_word':self.key_word})

        cur_page = page_data.get('currentPage')
        total_page = page_data.get('totalPage')

        if cur_page < total_page - 1:
            
            self.request_body['data']['mainSearchPcConditionForm'][
                'currentPage'] += 1
            print(self.request_body)
            yield scrapy.Request(url=self.url,
                        method='POST',
                        body=json.dumps(self.request_body),
                        headers=self.request_headers,
                        callback=self.basic_parse)