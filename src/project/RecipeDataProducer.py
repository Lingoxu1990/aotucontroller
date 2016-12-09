#!/usr/bin/env python
# -*- coding: utf-8 -*-
import InPutMessage
import OutPutMessage
import sqlite3
import ConfigParser
import logging
import time
import datetime
import SqliteUtil
import platform
ISOTIMEFORMAT ='%Y-%m-%d %X'

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

cloudColumnsStr = cf.get('uploader','CloudColumns')
cloudColumns = cloudColumnsStr.split(',')
LocalColumnsStr = cf.get('uploader','LocalColumns')
localColumns = LocalColumnsStr.split(',')


# 日志定义
logger = logging.getLogger("RecipeDataProducer")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("RecipeDataProducer.log")

# 设置文件handler 的日志输出等级
fh.setLevel(logging.DEBUG)

# 创建流式handler
ch = logging.StreamHandler()
# 设置流式hander  的输出等级
ch.setLevel(logging.ERROR)

# 配置格式化
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# 应用输出格式至 流式输出
ch.setFormatter(formatter)
# 应用输出格式至 文件输出
fh.setFormatter(formatter)

# 将输出器增加至logger(executor)
logger.addHandler(ch)
logger.addHandler(fh)



def getRecipeStartTime():
    # get ALL recipe info
    sql = 'SELECT region_guid,seq,start_time from private_recipe_index WHERE region_guid=(SELECT region_guid  FROM table_region GROUP BY region_guid)'


    result = SqliteUtil.getSqliteData(sql,'')

    if len(result)==0:
        logger.error('there is no recipe for the region :' + sql)
    else:
        logger.debug('the query sql is : ' + sql)

    return result

def getRecipePeriod(recipeStartTime):

    logger.debug('the start time of the recipe : ' + recipeStartTime)
    logger.debug('the local time of the system : ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    hostTime = time.localtime()
    startTime = time.strptime(recipeStartTime, ISOTIMEFORMAT)

    #caculate the days between startTime and hostTime
    indexDay = (datetime.date(hostTime.tm_year, hostTime.tm_mon, hostTime.tm_mday) - datetime.date(startTime.tm_year,startTime.tm_mon,startTime.tm_mday)).days

    #caculate the hours between startTime and hostTime
    indexHour = hostTime.tm_hour- startTime.tm_hour

    #transform  the info of day and hour to hours
    totalHours= indexDay*24+indexHour

    # caculate the day of the recipe
    recipeDay = 1+totalHours/24
    # caculate the period  of recipe day
    recipeHour = totalHours%24

    if(recipeHour==0):
        recipeHour=1

    logger.debug('the goal day of the recipe : %d , the goal hour of the recipe : %d'%(recipeDay,recipeHour))

    print recipeDay, recipeHour
    return recipeDay, recipeHour

def getRecipeRealTimeData(recipeDay, recipeHour):


    logger.debug('the realtime of the recipe is  day:%d , hour:%d '%(recipeDay, recipeHour))

    sql = 'SELECT * FROM private_recipe_data WHERE day=\''+str(recipeDay) +'\' AND start_time <=\''+str(recipeHour)+'\' AND end_time>=\''+str(recipeHour)+'\''

    logger.debug('the query sql is : ' + sql)

    result = SqliteUtil.getSqliteData(sql, '')

    logger.debug('the real-time recipe data : \n'+str(result))

    return result

def getTheMiddleValue(recipeData):
    global cloudColumns
    global localColumns
    resultValue={}

    for i, column in enumerate(cloudColumns):
        temp = 0.0
        index = 0
        temps = []

        for key in recipeData:

            if column in key:
                temp = temp + float(recipeData[key])
                temps.append(float(recipeData[key]))
                index = index + 1

        middleValue = temp / index

        columnName = localColumns[i]

        resultValue[columnName] = (middleValue, abs(temps[1] - temps[0]) / 2)


    return resultValue

def produce():
    # gets RecipeInfo about all regions and their start time
    recipeInfo = getRecipeStartTime()
    print recipeInfo

    # gets available data for the next step: get RealTime recipe data, the param  is the recipe-day and recipe-hour
    recipeDay, recipeHour=getRecipePeriod(recipeInfo[0]['start_time'])

    seq = int(recipeInfo[0]['seq'])

    # gets the real time recipe data,the function returns a list ,now we get the first one
    recipeDatas = getRecipeRealTimeData(recipeDay + seq - 1, recipeHour)

    if len(recipeDatas)==0:
        logger.error('there is no correspond data in this recipe')
        return []
    else:
        recipeMiddleValue =getTheMiddleValue(recipeDatas[0])
        return recipeMiddleValue