#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sqlite3
import logging
import ConfigParser
import httplib, urllib
import platform

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

recordPath= cf.get('dbPath','record')
recordSql = cf.get('dbPath','recordSql')

caches=[]


def getSqliteData(sql=recordSql):


    try:
     conn = sqlite3.connect(recordPath)
     cursor = conn.cursor()
     cursor.execute(sql)
     meta = cursor.description
     columns = {}

     for i,value in enumerate(meta):

         columns[i]=value[0]

     result = cursor.fetchall()


     return_result = []
     for data in result:
         temp=dict()
         for index in range(len(columns)):
             temp[columns[index]]=data[index]
         return_result.append(temp)


     conn.close()
    except StandardError ,e:
        logging.exception(e)
        return []

    return return_result


def cacheCheck(json):

    if len(caches)==0:
         caches.append(json)
         return json
    else:
        cacheTime = caches[0]['record_time']
        queryTime = json['record_time']

        if cacheTime==queryTime:
            # 如果查询结果与缓存一致,该数据不上传
            return {}
        else:
            # 如果查询结果与缓存不一致,更新缓存,上传最新数据
            caches.pop()
            caches.append(json)
            return caches[0]


def postData(json):

        httpClient = None

        params = urllib.urlencode({'name': 'tom', 'age': 22})
        headers = {"Content-type": "application/json"
            , "Accept": "*/*"}

        httpClient = httplib.HTTPConnection("localhost", 8080, timeout=30)
        httpClient.request("POST", "/download", params, headers)

        response = httpClient.getresponse()
        print response.status
        print response.reason
        print response.read()
        print response.getheaders()  # 获取头信息

        if httpClient:
           httpClient.close()


postData({'aaa':0})


