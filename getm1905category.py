# -*- coding: utf-8 -*-

"""
获取m1905的分类
2016年7月21日 10:27:19
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

if __name__ == '__main__':
    url = 'http://www.1905.com/mdb/film/search/'
    headers = Headers.getHeaders()
    req = requests.get(url, headers=headers, timeout=30)
    html = req.content
    seletor = etree.HTML(html)
    category = seletor.xpath('//dl[@class="srhGroup clear"][2]/descendant::dd')

    mysqlDao = MysqlDao()
    for c in category:
        category_names = c.xpath('a[1]/text()')
        category_urls = c.xpath('a[1]/@href')
        if len(category_names) > 0 and len(category_urls) > 0:
            category_name = category_names[0]
            category_url = Config.url_main + category_urls[0]
            sql = 'insert ignore into m1905_category (`category`,`url`,`created_at`) VALUES (%s,%s,%s)'
            created_at = time.strftime('%Y-%m-%d %H:%M:%S')
            print(category_name)
            print(category_url)
            values = (category_name, category_url, created_at)
            mysqlDao.executeValues(sql, values)
    mysqlDao.close()
