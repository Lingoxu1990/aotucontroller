#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sqlite3
import ConfigParser
import logging
import uuid
import io
import platform

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

cf.get('executor','source')

def getSqliteData(sql,agrs):


    try:
     conn = sqlite3.connect('../recipe.db')
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

def updateSqlData(sql):
    print sql
    conn = sqlite3.connect('../recipe.db')
    cursor = conn.cursor()
    cursor.execute(sql)
    a=cursor.rowcount
    conn.commit()
    return a

def Insert(params):

   # param = ('1','illuminance','0','0')
   # param2 = ('2','illuminance','0','0')
   # param3 = ('3','illuminance','0','0')
   # params = []
   # params.append(param)
   # params.append(param2)
   # params.append(param3)




   sql = 'INSERT INTO map_line (id,param,sensor_value,device_value) VALUES (?,?,?,?)'
   # sqlForTable= 'SELECT name FROM sqlite_master WHERE type=\'table\' AND name=\'map_line\''
   # result = getSqliteData(sqlForTable,'')
   #
   # if len(result)==0:
   InitMapLine()

   # sqlForDelete = 'DELETE FROM map_line'

   conn = sqlite3.connect('../recipe.db')

   cursor = conn.cursor()

   # cursor.execute(sqlForDelete)
   # conn.commit()

   cursor.executemany(sql,params)

   conn.commit()
   conn.close()

def InitMapLine():

    conn = sqlite3.connect('../recipe.db')

    dropSql = 'DROP table IF EXISTS map_line'
    createSql = 'CREATE TABLE "map_line\" ("id" VARCHAR(32,0) NOT NULL,"param" VARCHAR(32,0),"sensor_value" real(32,2),"device_value" integer(32,0),PRIMARY KEY("id"))'
    cursor = conn.cursor()
    cursor.execute(dropSql)
    cursor.execute(createSql)
    conn.commit()
    conn.close()

def test():

    params =[]
    for index in range(256):
       param = []
       param.append(str(uuid.uuid1()))
       param.append('illuminance')
       param.append(str(index*0.0))
       param.append(str(index))
       params.append(tuple(param))

    print params
    Insert(params)

# 该方法用于查找当前网关下的传感器数据
def getSensorRealtime(sql):

    try:
        conn = sqlite3.connect('/root/JenNet_File/Record_Data.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        meta = cursor.description
        columns = {}

        for i, value in enumerate(meta):
            columns[i] = value[0]

        result = cursor.fetchall()

        return_result = []
        for data in result:
            temp = dict()
            for index in range(len(columns)):
                temp[columns[index]] = data[index]
            return_result.append(temp)

        conn.close()
    except StandardError, e:
        logging.exception(e)
        return []

    return return_result

# 该方法用于查找网关ID,同时可以用于查找当前网关下的设备信息
def getTheGatewayId(sql):
    try:
        conn = sqlite3.connect('/root/JenNet_File/Basis_Data.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        meta = cursor.description
        columns = {}

        for i, value in enumerate(meta):
            columns[i] = value[0]
        result = cursor.fetchall()

        return_result = []
        for data in result:
            temp = dict()
            for index in range(len(columns)):
                temp[columns[index]] = data[index]
            return_result.append(temp)

        conn.close()
    except StandardError, e:
        logging.exception(e)
        return []

    return return_result

def getTheFaileQueue():

    sql = 'SELECT * FROM table_sensor_record'
    try:
        conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/FailedQueue.db')
        cursor = conn.cursor()
        cursor.execute(sql)
        meta = cursor.description
        columns = {}

        for i, value in enumerate(meta):
            columns[i] = value[0]

        result = cursor.fetchall()

        return_result = []
        for data in result:
            temp = dict()
            for index in range(len(columns)):
                temp[columns[index]] = data[index]
            return_result.append(temp)

        conn.close()
    except StandardError, e:
        logging.exception(e)
        return []

    return return_result

def appendMembersIntoTheFailedQueue(list):
    sql = 'insert into table_sensor_record (id,record_guid,table_device_guid,record_time,air_temperature,air_humidity,carbon_dioxide,illuminance,ppfd,liquid_ph,liquid_conductivity,liquid_doc,substrate_ph,substrate_conductivity,substrate_doc,lai,substrate_temperature,substrate_humidity,reserve01,reserve02,account_id) VALUES ( ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?,? )'

    valueList=[]
    for o in list:
        templist=[]
        templist.append(o['id'])
        templist.append(o['record_guid'])
        templist.append(o['table_device_guid'])
        templist.append(o['record_time'])
        templist.append(o['air_temperature'])
        templist.append(o['air_humidity'])
        templist.append(o['carbon_dioxide'])
        templist.append(o['illuminance'])
        templist.append(o['ppfd'])
        templist.append(o['liquid_ph'])
        templist.append(o['liquid_conductivity'])
        templist.append(o['liquid_doc'])
        templist.append(o['substrate_ph'])
        templist.append(o['substrate_conductivity'])
        templist.append(o['substrate_doc'])
        templist.append(o['lai'])
        templist.append(o['substrate_temperature'])
        templist.append(o['substrate_humidity'])
        templist.append(o['reserve01'])
        templist.append(o['reserve02'])
        templist.append(o['account_id'])
        valueList.append(templist)


    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/FailedQueue.db')
    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def removeTheMemberFromTheFailedQueue(list):
    sql = 'delete from table_sensor_record WHERE id=?'
    valueList = []
    for o in list:
        templist = []
        templist.append(o['id'])
        valueList.append(templist)

    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/FailedQueue.db')
    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def removeOneMemberFromTheFailedQueue(pk):

    sql = 'delete from table_sensor_record WHERE  id= \'%s\''%(pk)

    print sql

    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/FailedQueue.db')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def appendMembersIntoTheTotaleQueue(list):
    sql = 'insert into total_records(id,record_guid,table_device_guid,record_time,air_temperature,air_humidity,carbon_dioxide,illuminance,ppfd,liquid_ph,liquid_conductivity,liquid_doc,substrate_ph,substrate_conductivity,substrate_doc,lai,substrate_temperature,substrate_humidity,reserve01,reserve02,account_id) VALUES ( ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?,? )'

    valueList=[]
    for o in list:
        templist = []
        templist.append(o['id'])
        templist.append(o['record_guid'])
        templist.append(o['table_device_guid'])
        templist.append(o['record_time'])
        templist.append(o['air_temperature'])
        templist.append(o['air_humidity'])
        templist.append(o['carbon_dioxide'])
        templist.append(o['illuminance'])
        templist.append(o['ppfd'])
        templist.append(o['liquid_ph'])
        templist.append(o['liquid_conductivity'])
        templist.append(o['liquid_doc'])
        templist.append(o['substrate_ph'])
        templist.append(o['substrate_conductivity'])
        templist.append(o['substrate_doc'])
        templist.append(o['lai'])
        templist.append(o['substrate_temperature'])
        templist.append(o['substrate_humidity'])
        templist.append(o['reserve01'])
        templist.append(o['reserve02'])
        templist.append(o['account_id'])
        valueList.append(templist)


    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/log.db')
    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def appendMembersIntoTheFailedQueuePostLog(list):

    sql = 'insert into failed_post_from_failed_records (id,record_guid,table_device_guid,record_time,air_temperature,air_humidity,carbon_dioxide,illuminance,ppfd,liquid_ph,liquid_conductivity,liquid_doc,substrate_ph,substrate_conductivity,substrate_doc,lai,substrate_temperature,substrate_humidity,reserve01,reserve02,account_id)  VALUES ( ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?,? )'

    valueList=[]
    for o in list:
        templist = []
        templist.append(o['id'])
        templist.append(o['record_guid'])
        templist.append(o['table_device_guid'])
        templist.append(o['record_time'])
        templist.append(o['air_temperature'])
        templist.append(o['air_humidity'])
        templist.append(o['carbon_dioxide'])
        templist.append(o['illuminance'])
        templist.append(o['ppfd'])
        templist.append(o['liquid_ph'])
        templist.append(o['liquid_conductivity'])
        templist.append(o['liquid_doc'])
        templist.append(o['substrate_ph'])
        templist.append(o['substrate_conductivity'])
        templist.append(o['substrate_doc'])
        templist.append(o['lai'])
        templist.append(o['substrate_temperature'])
        templist.append(o['substrate_humidity'])
        templist.append(o['reserve01'])
        templist.append(o['reserve02'])
        templist.append(o['account_id'])
        valueList.append(templist)

    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/log.db')

    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def appendMembersIntoTheDataQueuePostLog(list):

    sql = 'insert into failed_post_from_real_records (id,record_guid,table_device_guid,record_time,air_temperature,air_humidity,carbon_dioxide,illuminance,ppfd,liquid_ph,liquid_conductivity,liquid_doc,substrate_ph,substrate_conductivity,substrate_doc,lai,substrate_temperature,substrate_humidity,reserve01,reserve02,account_id) VALUES ( ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?,? )'

    valueList=[]
    for o in list:
        templist = []
        templist.append(o['id'])
        templist.append(o['record_guid'])
        templist.append(o['table_device_guid'])
        templist.append(o['record_time'])
        templist.append(o['air_temperature'])
        templist.append(o['air_humidity'])
        templist.append(o['carbon_dioxide'])
        templist.append(o['illuminance'])
        templist.append(o['ppfd'])
        templist.append(o['liquid_ph'])
        templist.append(o['liquid_conductivity'])
        templist.append(o['liquid_doc'])
        templist.append(o['substrate_ph'])
        templist.append(o['substrate_conductivity'])
        templist.append(o['substrate_doc'])
        templist.append(o['lai'])
        templist.append(o['substrate_temperature'])
        templist.append(o['substrate_humidity'])
        templist.append(o['reserve01'])
        templist.append(o['reserve02'])
        templist.append(o['account_id'])
        valueList.append(templist)


    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/log.db')
    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def appendMembersIntoTheTotalFailedQueue(list):
    sql = 'insert into failed_records (id,record_guid,table_device_guid,record_time,air_temperature,air_humidity,carbon_dioxide,illuminance,ppfd,liquid_ph,liquid_conductivity,liquid_doc,substrate_ph,substrate_conductivity,substrate_doc,lai,substrate_temperature,substrate_humidity,reserve01,reserve02,account_id) VALUES ( ?, ?, ?, ?, ?,?, ?, ?,?, ?, ?,?, ?, ?, ?, ?, ?,?, ?, ?,? )'

    valueList=[]
    for o in list:
        templist = []
        templist.append(o['id'])
        templist.append(o['record_guid'])
        templist.append(o['table_device_guid'])
        templist.append(o['record_time'])
        templist.append(o['air_temperature'])
        templist.append(o['air_humidity'])
        templist.append(o['carbon_dioxide'])
        templist.append(o['illuminance'])
        templist.append(o['ppfd'])
        templist.append(o['liquid_ph'])
        templist.append(o['liquid_conductivity'])
        templist.append(o['liquid_doc'])
        templist.append(o['substrate_ph'])
        templist.append(o['substrate_conductivity'])
        templist.append(o['substrate_doc'])
        templist.append(o['lai'])
        templist.append(o['substrate_temperature'])
        templist.append(o['substrate_humidity'])
        templist.append(o['reserve01'])
        templist.append(o['reserve02'])
        templist.append(o['account_id'])
        valueList.append(templist)


    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/log.db')
    cursor = conn.cursor()
    cursor.executemany(sql, valueList)
    conn.commit()
    conn.close()

def clearLog(tableName):
    sql = 'delete from \'%s\'' % (tableName)

    conn = sqlite3.connect('/root/JenNet_File/sonneteckAutoController/src/project/log.db')
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()

# test()