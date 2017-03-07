# -*- coding: utf-8 -*-
import json
import codecs
import pymysql
import pymysql.cursors
from twisted.enterprise import adbapi
from datetime import datetime
from hashlib import md5
import logging
import sys
from formatData import formatData

class MaizePipeline(object):
    def __init__(self):
        self.file = codecs.open('c:/tools/priceData.json', 'w',"utf-8")
    def process_item(self, item, spider):
        line = json.dumps(dict(item)) + "\n"
        self.file.write(line)
        return item

class Maize2DbPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        dbargs = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            passwd=settings['MYSQL_PASSWD'],
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
        )
        dbpool = adbapi.ConnectionPool('pymysql', **dbargs)
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self._conditional_insert, item, spider)
        query.addErrback(self._handle_error, item, spider)
        return item

    # 将每行更新或写入数据库中
    def _conditional_insert(self, conn, item, spider):
        link = item['link'][0]
        check_code = self._get_linkmd5id(item)
        now = datetime.now().replace(microsecond=0).isoformat(' ')
        date, priceDict = formatData(item)
        conn.execute('''
                select * from price_corp_name
        ''')
        corp_list = conn.fetchall()
        corp_dict = {}
        for corp in corp_list:
            corp_dict[corp['corp_name']] = corp['corp_code']

        conn.execute("""
                select 1 from price_trend where check_code = %s
        """, (check_code,))
        ret = conn.fetchone()

        if ret:
            pass
        else:
            sql = 'insert into price_trend(prod_name, corp_code, pdate, price, check_code, update_time)' \
                  ' values (%s,%s,%s,%s,%s,%s)'
            for (corp_name, price) in priceDict.items():
                content = ['玉米', corp_dict[corp_name], date, price, check_code, now]
                conn.execute(sql, content)

    # 获取url的md5编码
    def _get_linkmd5id(self, item):
        # url进行md5处理，为避免重复采集设计
        return md5(item['link'][0].encode('utf-8')).hexdigest()

    # 异常处理
    def _handle_error(self, failure, item, spider):
        logging.error(failure)



# 数据库设计有问题
# class Maize2DbPipeline(object):
#     def __init__(self, dbpool):
#         self.dbpool = dbpool
#
#     @classmethod
#     def from_settings(cls, settings):
#         dbargs = dict(
#             host=settings['MYSQL_HOST'],
#             db=settings['MYSQL_DBNAME'],
#             user=settings['MYSQL_USER'],
#             passwd=settings['MYSQL_PASSWD'],
#             charset='utf8mb4',
#             cursorclass=pymysql.cursors.DictCursor,
#         )
#         dbpool = adbapi.ConnectionPool('pymysql',**dbargs)
#         return cls(dbpool)
#
#     def process_item(self, item, spider):
#         query = self.dbpool.runInteraction(self._conditional_insert, item, spider)
#         query.addErrback(self._handle_error, item, spider)
#         return item
#
#     # 将每行更新或写入数据库中
#     def _conditional_insert(self, conn, item, spider):
#         link = item['link'][0]
#         linkmd5id = self._get_linkmd5id(item)
#         # print('--------------------------------------------')
#         # print(linkmd5id)
#         # print('--------------------------------------------')
#         now = datetime.now().replace(microsecond=0).isoformat(' ')
#         date, priceDict = formatData(item)
#         conn.execute('''
#                 select * from maize_corp_name
#         ''')
#         corp_list = conn.fetchall()
#         corp_dict = {}
#         for corp in corp_list:
#             corp_dict[corp['maize_corp_code']] = corp['maize_corp_name']
#
#         conn.execute("""
#                 select 1 from maize_prize where prize_code = %s
#         """, (linkmd5id,))
#         ret = conn.fetchone()
#
#         if ret:
#             sql = 'update maize_prize set '
#             content = []
#             for (k, v) in corp_dict.items():
#                 sql += k
#                 sql += '=%s, '
#                 if v in priceDict.keys():
#                     content.append(priceDict[v])
#                 else:
#                     content.append(0)
#             sql += 'pdate = %s, updatetime = %s where prize_code = %s'
#             content.append(date)
#             content.append(now)
#             content.append(linkmd5id)
#
#             conn.execute(sql, content)
#         else:
#             sql = 'insert into maize_prize(prize_code, link, pdate, updatetime,'
#             str1 = ''
#             str2 = '%s,%s,%s,%s,'
#             content = [linkmd5id, link, date, now]
#             for (k, v) in corp_dict.items():
#                 str1 = str1 + k + ', '
#                 str2 += '%s, '
#                 if v in priceDict.keys():
#                     content.append(priceDict[v])
#                 else:
#                     content.append(0)
#             sql = sql + str1[:-2] + ')values(' + str2[:-2] + ')'
#             conn.execute(sql, content)
#
#     # 获取url的md5编码
#     def _get_linkmd5id(self, item):
#         # url进行md5处理，为避免重复采集设计
#         return md5(item['link'][0].encode('utf-8')).hexdigest()
#
#     # 异常处理
#     def _handle_error(self, failure, item, spider):
#         logging.error(failure)
