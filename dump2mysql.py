# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from datetime import datetime
import pymysql


class Mysql(object):
    def __init__(self, table):
        self.db = pymysql.connect(host='127.0.0.1', user='root', password='xxx', db='washer', charset='utf8')
        self.table = table

    def dump(self, item):
        try:
            with self.db.cursor() as cursor:
                # Create a new record
                columns = ", ".join(item.fields)
                values_template = ", ".join(["%s"] * len(item.fields))
                values = tuple(item[k] for k in item.fields)

                sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table, columns, values_template)
                cursor.execute(sql, values)
            # connection is not autocommit by default. So you must commit to save
            # your changes.
            self.db.commit()
        except:
            self.db.rollback()
        return item


