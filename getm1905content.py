# -*- coding: utf-8 -*-

import sys
import time
import requests
from lxml import etree
from mysqlpooldao import MysqlDao
from redispooldao import RedisDao
from headers import Headers
from config import Config
import simplejson
import threading
import Queue

reload(sys)
sys.setdefaultencoding('utf8')


class Worker(threading.Thread):
    def __init__(self, redisDao, mysqlDao):
        threading.Thread.__init__(self)
        self._queue = queue
        self._mysqlDao = mysqlDao

    def run(self):
        while True:
            print (self.getName())
            if self._queue.empty():
                break
            ret_json = self._queue.get()
            ret = simplejson.loads(ret_json)
            id = ret[0]
            category_id = ret[1]
            content_url = ret[2]
            headers = Headers().getHeaders()
            print(content_url)
            try:
                req = requests.get(content_url, headers=headers, timeout=60)
                if req.status_code == 200:
                    html = req.content
                    selector = etree.HTML(html)
                    titles = selector.xpath('//*[@class="fl"]/a[1]/text()')
                    nicks = selector.xpath('//*[@class="fl"]/span[1]/a[1]/text()')
                    play_urls = selector.xpath('//*[@class="redBtn"]/a[1]/@href')
                    contents = []
                    contents_li = selector.xpath('//*[@class="movStaff line_BSld"]/li')
                    for c_li in contents_li:
                        c_temp = c_li.xpath('descendant::text()')
                        if len(c_temp) > 0:
                            contents.append(c_temp)
                    imgs = selector.xpath('//*[@class="imgBAyy db"]/descendant::img[1]/@src')
                    title = play_url = content = img = nick = ''
                    if len(titles) > 0:
                        title = titles[0]
                    if len(nicks) > 0:
                        nick = nicks[0]
                    title = title + ',' + nick
                    if len(play_urls) > 0:
                        play_url = play_urls[0]
                    if len(imgs) > 0:
                        img = imgs[0]
                    content = simplejson.dumps(contents)
                    created_at = time.strftime('%Y-%m-%d %H:%M:%S')
                    if img != '':
                        # 存入content
                        sql = 'insert ignore into m1905_content (`category_id`,`title`,`content`,`play_url`,`img`,`url`,`created_at`) VALUES (%s,%s,%s,%s,%s,%s,%s)'
                        values = (category_id, title, content, play_url, img, content_url, created_at)
                        print(title)
                        self._mysqlDao.executeValues(sql, values)
            except:
                self._mysqlDao = MysqlDao()
            if img != '':
                # url置1
                sql = 'update m1905_url set `status`=1 where `id`=' + str(id)
                self._mysqlDao.execute(sql)


if __name__ == '__main__':
    mysqlDao = MysqlDao()
    # redisDao = RedisDao()
    queue = Queue.Queue()
    while True:

        sql = 'select `id`,`category_id`,`url` from m1905_url WHERE `status`=0 limit 0,1000'
        ret = mysqlDao.execute(sql)
        print(ret)
        # 如果取出来为空,程序结束
        if len(ret) == 0:
            break
        # 将mysql的数据存入redis队列
        for r in ret:
            r_json = simplejson.dumps(r)
            queue.put(r_json)
        # 开始多线程
        worker_num = 2
        threads = []
        for x in xrange(0, worker_num):
            threads.append(Worker(queue, mysqlDao))
        for t in threads:
            t.setDaemon(True)
            t.start()
            # time.sleep(1)
        for t in threads:
            t.join()
        threads = []
    mysqlDao.close()
    print('game over')
