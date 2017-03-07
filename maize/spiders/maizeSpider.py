# coding=utf-8
__author__ = 'JoRay'

from maize.items import MaizeItem
import logging
import scrapy
from scrapy.http import Request
from scrapy.selector import Selector
import re

url = 'http://www.yz88.cn/sz/ShowClass.asp?ClassID=25&page='


class maizeSpider(scrapy.Spider):
    name = "maize"
    # allowed_domains = [""]
    start_urls = [
        "http://www.yz88.cn/sz/List_25.shtml"
    ]

    def parse(self, response):

        if response.status == 200:
            content = response.xpath(
                '//*[@id="box"]/div[8]/ul/li/a[2][contains(@title,"日山东玉米")]|//*[@id="box"]/div[8]/ul/li/a[2][contains(@title,"山东今日玉米")]')
                # 'body/div[@id="cont"]/div[@id="list"]/ul[@class="list"]/li/a[2][contains(@title,"日山东玉米")]|body/div[@id="cont"]/div[@id="list"]/ul[@class="list"]/li/a[2][contains(@title,"山东今日玉米")]')
            for p in content:
                item = MaizeItem()
                item['link'] = p.xpath('./@href').extract()
                item['title'] = p.xpath('string(.)').extract()
                print(item['title'])
                item['priceDesc'] = []
                contentUrl = p.xpath('./@href').extract()[0]
                yield scrapy.Request(contentUrl, meta={'item': item}, callback=self.parseMaizePrice)

            nextpage = response.xpath('//*[@id ="box"]/div[8]/div[2]/a[text()="下一页"]/@href').extract()
            if (nextpage):
                nextpage = nextpage[0]
                print(nextpage)
                yield scrapy.Request(nextpage, callback=self.parse)

    def parseMaizePrice(self, response):

        item = response.meta['item']
        # html = response.body
        # # html = html.decode("gbk",'ignore')
        # words = r'<A title="" href="http://www.yz88.cn/sz/List_25.shtml"><B>(.*?)</B></A>'
        # pattern = re.compile(words)
        # re.sub(pattern, r'\1', html)
        # priceDesc = Selector(text=html).xpath('//*[@id="art_ls"]/div[@class="content"]/p/text()').extract()
        pContent = response.xpath('//*[@id="art_ls"]/div[@class="content"]/p[not(@align)]')
        for text in pContent:
            priceDesc = text.xpath('string(.)').extract()
            item['priceDesc'].extend(priceDesc)

        nextPricePage = response.xpath(
            '//*[@id="art_ls"]/div[@class="content"]/p[@align="center"]/b/a[text()="下一页"]/@href')
        if (nextPricePage):
            nextPricePage = response.urljoin(nextPricePage[0].extract())
            return scrapy.Request(nextPricePage, meta={'item': item}, callback=self.parseMaizePrice)
        else:
            return item
