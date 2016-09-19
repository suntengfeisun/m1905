# -*- coding: utf-8 -*-

"""
获取m1905的所有电影的url
"""

import sys
import requests
import time
from config import Config
from lxml import etree
from mysqlpooldao import MysqlDao
from headers import Headers

reload(sys)
sys.setdefaultencoding('utf8')


def getLastPage(url):
    last_page = 10
    headers = Headers.getHeaders()
    req = requests.get(url, headers=headers, timeout=30)
    if req.status_code == 200:
        html = req.content
        selector = etree.HTML(html)
        movie_count_text = selector.xpath('//div[@class="termsBox"]/div[1]/text()')
        if len(movie_count_text) > 0:
            movie_count = int(filter(str.isdigit, movie_count_text[0].encode('utf8')))
            last_page = int(movie_count / 30)
            if movie_count % 30 > 0:
                last_page = last_page + 1
    return last_page


def getContentUrl(url, category_id, mysqlDao):
    headers = Headers.getHeaders()
    req = requests.get(url, headers=headers, timeout=60)
    if req.status_code == 200:
        html = req.content
        selector = etree.HTML(html)
        content_urls = selector.xpath('//ul[@class="inqList pt18"]/li/a/@href')
        content_urls.reverse()
        for content_url in content_urls:
            content_url = Config.url_main + content_url
            created_at = time.strftime('%Y-%m-%d %H:%M:%S')
            sql = 'insert ignore into m1905_url (`category_id`,`url`,`status`,`created_at`) VALUES (%s,%s,%s,%s)'
            values = (category_id, content_url, 0, created_at)
            mysqlDao.executeValues(sql, values)


if __name__ == '__main__':
    mysqlDao = MysqlDao()
    sql = 'select `id`,`url` FROM m1905_category'
    categorys = mysqlDao.execute(sql)
    print(categorys)
    for category in categorys:
        category_id = category[0]
        category_url = category[1]
        last_page = getLastPage(category_url)
        while True:
            if last_page < 1:
                break
            url = category_url + 'o0d0p%s.html' % (last_page)
            last_page = last_page - 1
            print(url)
            getContentUrl(url, category_id, mysqlDao)
    mysqlDao.close()
    print('game over')
