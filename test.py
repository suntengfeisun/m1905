# -*- coding: utf-8 -*-

from redispooldao import RedisDao

aaa = ''
redisDao = RedisDao()

#redisDao.rpush('aaa','bbb')
aaa = redisDao.lpop('aaa')

print(aaa)