# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals

# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
import redis,random

from twisted.internet.error import TCPTimedOutError,TimeoutError

class LpJobSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class LpJobDownloaderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)

class ProxiesMiddleware:
    def __init__(self, settings):
        # ipproxy_pool数据库
        self.redis_pool = redis.ConnectionPool(host=settings.get('REDIS_HOST'),port=settings.get('REDIS_PORT'),password=settings.get('REDIS_PASSWORD'),db=settings.get('REDIS_DB_COMMON'),decode_responses=True)
        self.redis_db = redis.Redis(connection_pool=self.redis_pool)
        pass

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    # def process_request(self, request, spider):
    #     ipproxy_list = list(self.redis_db.hgetall('ipproxy_list').keys())
    #     random_ip = ipproxy_list[random.randrange(0,len(ipproxy_list))]
    #     print(random_ip)
    #     # request.meta['proxy'] = "http://188.132.222.6:8080"
    #     request.meta['proxy'] = random_ip
    
    def process_exception(self,request,exception,spider):
        if isinstance(exception,TimeoutError):
            self.process_request_back(request,spider)
            return request

        elif isinstance(exception,TCPTimedOutError):
            self.process_request_back(request,spider)
            return request

    def process_request_back(self,request,spider):
        ipproxy_list = list(self.redis_db.hgetall('ipproxy_list').keys())
        random_ip = ipproxy_list[random.randrange(0,len(ipproxy_list))]
        # request.meta['proxy'] = "http://188.132.222.6:8080"
        request.meta['proxy'] = random_ip