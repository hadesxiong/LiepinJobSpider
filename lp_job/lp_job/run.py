from twisted.internet import reactor

from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner

import sys,itertools,time

sys.path.append('../')

from lp_job.spiders.main import jobItemSpider

if __name__ == "__main__":

  settings = get_project_settings()
  keyword_list = settings.get('KEY_WORD')
  city_list = settings.get('CITY')
  word_city = list(itertools.product(keyword_list, city_list))

  runner = CrawlerRunner(settings)
  for each_args in word_city:
    print(each_args)
    runner.crawl(jobItemSpider,key_word=each_args[0],city=each_args[1])

  d= runner.join()
  d.addBoth(lambda _:reactor.stop())

  reactor.run()
  