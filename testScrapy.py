from pathlib import Path
import scrapy
import json
from scrapy.crawler import CrawlerProcess
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule
from scrapy.utils.response import get_base_url
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
import os 
from scrapy.pipelines.files import FilesPipeline
import time
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from twisted.internet import task
import scrapy
from scrapy.utils.project import get_project_settings
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from twisted.internet import task
import time
import logging

path = '/Users/cyril/Documents/Works/DTCC/sample/test/'
timeout = 10
logging.basicConfig(level=logging.INFO)

class CustomFilePipelines(FilesPipeline):
    def file_path(self, request, response=None, info=None, *, item=None):
        return item.get('name')+'.zip'

class FileDownloadItem(scrapy.Item):
    file_urls = scrapy.Field()
    files = scrapy.Field()

class TestSpider(scrapy.Spider):
    name = 'test'


    start_urls = [
            'https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/slices/SEC_GENERAL_SLICE_CREDITS.HTML',
            #'https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/slices/SEC_GENERAL_SLICE_EQUITIES.HTML',
            #'https://kgc0418-tdw-data-0.s3.amazonaws.com/sec/slices/SEC_GENERAL_SLICE_RATES.HTML',
        ]
    
    def compare_response(self, responseJson) :
        print(">>> IN COMPARE > ")
        # Create an empty list to store the differences
        differences = []
        #print(responseJson)
        #if os.stat("/Users/cyril/Documents/Works/DTCC/sample/test/latest_items.json").st_size == 0:
        if os.stat(path + "latest_items.json").st_size == 0:
            current_data = json.loads('' or 'null')
        else:
            with open(path + 'latest_items.json','r',encoding='utf-8') as f:
                current_data = json.loads(f.read())

        if current_data is None: 
            for item in responseJson:
                differences.append(item)
        else :
            for element in responseJson:
                if element not in current_data:
                #if element not in responseJson:
                    differences.append(element)

        print("Compare > differences:")
        print(len(differences))
        if len(differences) > 0:
            with open(path + 'latest_items.json', 'w+') as writer:
                json_string = json.dumps(responseJson)
                writer.write(json_string)

        return differences


    def parse(self, response):
        #print("    >> URL " + response.url)
        print ("Site updated")
        l = response.css('a::text').getall()
        print("response Len:")
        print(len(l))
        diff = self.compare_response(l)
        print(diff)
        
        for link in response.xpath('.//a') :
            #loader = ItemLoader(item=FileDownloadItem(), selector=link)
            title = link.xpath(".//text()").get()
            if title in diff:
                relative_url = link.xpath(".//@href").extract_first()
                absolute_url = response.urljoin(relative_url)
                #loader.add_value("file_urls", absolute_url)
                #yield loader.load_item()
                yield {
                    'name':title,
                    'file_urls':[absolute_url]
                }
        print("END")
            

def run_spider():
    l.stop()
    setting = {
        'ITEM_PIPELINES': {
            'testScrapy.CustomFilePipelines': 1
        },
        'DOWNLOAD_TIMEOUT': 1500,
        'FILES_STORE': path + 'full',
        'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7'
    }
    print(":: STARTED <<"+time.strftime("%m/%d/%Y, %H:%M:%S", time.localtime())+">> ")
    runner = CrawlerRunner(settings=setting)
    d = runner.crawl(TestSpider)
    #d.addBoth(lambda _: reactor.stop())
    d.addBoth(lambda _: l.start(timeout, False))


if __name__ == "__main__":
    print("- START PROCESS -")
    l = task.LoopingCall(run_spider)
    l.start(timeout)
    reactor.run()