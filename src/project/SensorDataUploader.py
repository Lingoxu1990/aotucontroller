#!/usr/bin/env python
# -*- coding: utf-8 -*-
import SqliteUtil
import httplib
import urllib
import json
import uuid
import io
import time
import os
import sys
import platform
import threading
import ConfigParser
import hashlib
import traceback
import logging
import datetime
from logging.handlers import TimedRotatingFileHandler
from logging.handlers import RotatingFileHandler

#日志打印格式
log_fmt = '%(asctime)s\tFile \"%(filename)s\",line %(lineno)s\t%(levelname)s: %(message)s'
formatter = logging.Formatter(log_fmt)
logging.basicConfig(level=logging.DEBUG)
#创建TimedRotatingFileHandler对象
fileUpdaterHandler = TimedRotatingFileHandler(filename="/sbin/updateLog", when="D", interval=1, backupCount=2)
fileUpdaterHandler.setFormatter(formatter)
fileUpdater = logging.getLogger('update')
fileUpdater.addHandler(fileUpdaterHandler)


dataReaderHandler = TimedRotatingFileHandler(filename="/sbin/dataReaderLog", when="D", interval=1, backupCount=2)
dataReaderHandler.setFormatter(formatter)
dataReader = logging.getLogger('dataReader')
dataReader.addHandler(dataReaderHandler)

failedPostHandler = TimedRotatingFileHandler(filename="/sbin/failedPostLog", when="D", interval=1, backupCount=2)
failedPostHandler.setFormatter(formatter)
failedPost = logging.getLogger('failedPost')
failedPost.addHandler(failedPostHandler)

failedUpdateHandler = TimedRotatingFileHandler(filename="/sbin/failedUpdateLog", when="D", interval=1, backupCount=2)
failedUpdateHandler.setFormatter(formatter)
failedUpdate = logging.getLogger('failedUpdate')
failedUpdate.addHandler(failedPostHandler)

basisDataReaderHandler = TimedRotatingFileHandler(filename="/sbin/BasisDataReaderLog", when="D", interval=1, backupCount=2)
basisDataReaderHandler.setFormatter(formatter)
basisDataReader = logging.getLogger('BasisDataReader')
basisDataReader.addHandler(basisDataReaderHandler)

basisDataUpdateHandler = TimedRotatingFileHandler(filename="/sbin/basisDataUpdateLog", when="D", interval=1, backupCount=2)
basisDataUpdateHandler.setFormatter(formatter)
basisDataUpdate = logging.getLogger('basisDataUpdate')
basisDataUpdate.addHandler(basisDataUpdateHandler)




cf = ConfigParser.ConfigParser()

cf.read("/sbin/config.conf")

serverAddr = cf.get('uploader','addr')
serverPort =cf.get('uploader','port')
serverUri = cf.get('uploader','uri')

basisUpdateAddr = cf.get('BasisUpdater','addr')
basisUpdatePort = cf.get('BasisUpdater','port')
deviceUpdateUri = cf.get('BasisUpdater','deivceUri')
classUpdateUri = cf.get('BasisUpdater','classUri')
gatewayUpdateUri = cf.get('BasisUpdater','gatewayUri')

failed_post_from_real_records = cf.get('uploader','failed_post_from_real_records')
failed_post_from_failed_records = cf.get('uploader','failed_post_from_failed_records')
failed_records = cf.get('uploader','failed_records')
total_records = cf.get('uploader','total_records')

appId = cf.get('AG','appId')
token = cf.get('AG','token')
timestamp = cf.get('AG','timestamp')
md5Param = cf.get('AG','md5Param')

def getUtcTime():
    headers = {"Accept": "application/json"}

    httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
    httpClient.request("GET", "/timestamp", '', headers)

    response = httpClient.getresponse()
    if response.status == 200:
        tms = response.read()
        tms = json.loads(tms)
        aaa = tms['content']
        ltime = time.localtime(aaa)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", ltime)
        return timeStr
    else:
        return None

def getTheRealTimeData():

    sqlForSensorRecord = 'SELECT * FROM table_sensor_record'

    records = SqliteUtil.getSensorRealtime(sqlForSensorRecord)
    utcTime = getUtcTime()

    device_type='gateway'

    sqlForGateway = 'SELECT * FROM table_device WHERE device_type = \'%s\''%(device_type)

    gateway = SqliteUtil.getTheGatewayId(sqlForGateway)

    gatewayId = gateway[0]['gateway_id']

    accountId = gatewayId[0:8]

    for record in records:
        record['id']= str(uuid.uuid1())
        record['account_id']=accountId
        if utcTime!=None:
            record['record_time']=utcTime
        else:
            dataReader.debug("could not get the online utc time,use the original time instead.")
    return records

def getTheBaisData():
    sqlForDevice = 'SELECT * FROM table_device'
    deviceDatas = SqliteUtil.getTheGatewayId(sqlForDevice)
    gatewayId = deviceDatas[0]['gateway_id']
    accountId = gatewayId[0:8]
    for data in deviceDatas:
        data['account_id']=accountId

    sqlForChannel = 'SELECT * FROM table_channel'
    ChannelDatas = SqliteUtil.getTheGatewayId(sqlForChannel)
    for data in ChannelDatas:
        data['account_id'] = accountId

    AccountDataInfo = {}
    AccountDataInfo['gateway_id']=gatewayId
    AccountDataInfo['account_id']=accountId
    return deviceDatas,ChannelDatas,AccountDataInfo

def getTheFailedQueue():
    return SqliteUtil.getTheFaileQueue()


def appendIntoFailedQueue(jsonList):
    SqliteUtil.appendMembersIntoTheFailedQueue(jsonList)

def clearQueue(list):
    SqliteUtil.removeTheMemberFromTheFailedQueue(list)

def clearDpk(pk):
    SqliteUtil.removeOneMemberFromTheFailedQueue(pk)

def totalRecordLog(list):
    SqliteUtil.appendMembersIntoTheTotaleQueue(list)

def totalFailedLog(list):
    SqliteUtil.appendMembersIntoTheTotalFailedQueue(list)

def totalFailedQueuePostLog(list):
    SqliteUtil.appendMembersIntoTheFailedQueuePostLog(list)

def totalFailedDataPostLog(list):
    SqliteUtil.appendMembersIntoTheDataQueuePostLog(list)

def agSign():
    param={}
    httpClient = None

    try:
        headers = {"Accept": "application/json"}

        httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
        httpClient.request("GET", timestamp, '', headers)

        response = httpClient.getresponse()

        tms = response.read()

        tms = json.loads(tms)
        ts = tms['content']
        md5Eencoder = hashlib.md5()

        aaa = appId + str(ts) + token + md5Param
        md5Eencoder.update(aaa)
        sign = md5Eencoder.hexdigest()

        param = {'sign': sign, 'ts': ts, 'appID': appId}

        param = urllib.urlencode(param)

        return param

    except Exception,e:
        print e
    finally:
        if httpClient != None:
            httpClient.close()
            return  param



def upLoadFailed(params):
    # print params

    param = agSign()

    result = True

    params = json.dumps(params)

    headers = {"Content-type": "application/json"
        , "Accept": "*/*"}

    httpClient = None

    try:

        httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
        httpClient.request("POST", '%s?%s'%(serverUri,param), params, headers)

        response = httpClient.getresponse()

        if response.status != 200:

            result = False

        strResult = response.read()

        jsonReslut = json.loads(strResult)


        if jsonReslut['code'] != '0':


            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                pk = jsonReslut['content']
                clearDpk(pk)
                return result

        return result

    except Exception,e:
        print e
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result

def upLoadFailedWithoutAG(params):


    result = True

    params = json.dumps(params)

    headers = {"Content-type": "application/json"
        , "Accept": "*/*"}

    httpClient = None

    try:

        httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
        httpClient.request("POST", serverUri, params, headers)

        response = httpClient.getresponse()

        if response.status != 200:

            result = False

        strResult = response.read()

        jsonReslut = json.loads(strResult)


        if jsonReslut['code'] != '0':


            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                pk = jsonReslut['content']
                clearDpk(pk)
                return result

        return result

    except Exception,e:
        print e
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result


def upLoad(params):
    param = agSign()
    result=True
    params = json.dumps(params)
    headers = {"Content-type": "application/json"
        , "Accept": "application/json"}
    httpClient=None
    try:
     httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
     httpClient.request("POST", '%s?%s'%(serverUri,param), params, headers)
     response = httpClient.getresponse()
     if response.status!=200:
         result=False
     strResult = response.read()
     print strResult
     jsonReslut = json.loads(strResult)
     if jsonReslut['code']!='0':
         result=False
         if jsonReslut['message']=='DuplicateKeyException':
             pk = jsonReslut['content']
             # print 'the pk: '+str(pk) +' is duplicate '
             clearDpk(pk)
             return result
     return  result
    except Exception,e:
        failedPost.debug(e)
        result=False
    finally:
        if httpClient!=None:
          httpClient.close()
          return result

def upLoadWithoutAG(params):
    result = True
    params = json.dumps(params)
    headers = {"Content-type": "application/json"
        , "Accept": "application/json"}
    httpClient = None
    try:
        httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
        httpClient.request("POST", serverUri, params, headers)
        response = httpClient.getresponse()
        if response.status != 200:
            result = False
        strResult = response.read()
        print strResult
        jsonReslut = json.loads(strResult)
        if jsonReslut['code'] != '0':
            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                pk = jsonReslut['content']
                # print 'the pk: '+str(pk) +' is duplicate '
                clearDpk(pk)
                return result
        return result
    except Exception, e:
        failedPost.debug(e)
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result

def upDate(params,uri):
    result = True
    params = json.dumps(params)
    headers = {"Content-type": "application/json"
        , "Accept": "application/json"}
    httpClient = None
    try:
        httpClient = httplib.HTTPConnection(basisUpdateAddr, int(basisUpdatePort), timeout=3)
        httpClient.request("POST", uri, params, headers)
        # httpClient = httplib.HTTPConnection('localhost',8080,timeout=3)
        # httpClient.request("POST",'/upload/basis',params,headers)
        response = httpClient.getresponse()
        if response.status != 200:
            result = False
        strResult = response.read()
        print strResult
        jsonReslut = json.loads(strResult)
        if jsonReslut['code'] != '0':
            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                pk = jsonReslut['content']
                clearDpk(pk)
                return result
        return result
    except Exception, e:
        failedUpdate.debug(e)
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result

def uploadTest(params):
    # print params

    result = True

    params = json.dumps(params)

    print params

    headers = {"Content-type": "application/json"
        , "Accept": "*/*"}

    httpClient = None

    try:

        httpClient = httplib.HTTPConnection('localhost', 8080, timeout=2)
        httpClient.request("POST", serverUri, params, headers)

        response = httpClient.getresponse()

        if response.status != 200:
            result = False

        strResult = response.read()

        jsonReslut = json.loads(strResult)

        if jsonReslut['code'] != '0':
            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                pk = jsonReslut['content']
                failed = []
                for x in params:
                    if x['id'] == pk:
                        failed.append(x)
                return result
        return result

    except Exception:
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result

def failedUploader():
    while True:
        dataInQueue = getTheFailedQueue()

        print 'the failed data :' + str(dataInQueue)

        flagForQueue = True
        if len(dataInQueue) != 0:
            # flagForQueue = upLoadFailed(dataInQueue)
            flagForQueue=upLoadFailedWithoutAG(dataInQueue)

        if flagForQueue:
            print 'upload the failedQueue successfully, clear the uploaded data'
            clearQueue(dataInQueue)
        else:
            print 'failed to upload the failedQueue '

        time.sleep(10)

def realTimeUploader():
    statinfo = os.stat('/root/JenNet_File/Record_Data.db')
    cacheTime = statinfo.st_mtime
    currentTime = 0.0
    while True:
        flag = False
        for x in range(10):
            statinfo = os.stat('/root/JenNet_File/Record_Data.db')
            currentTime = statinfo.st_mtime
            if currentTime - cacheTime > 0:
                flag = True
                break
            else:
                time.sleep(1)

        ltime = time.localtime(cacheTime)
        timeStr = time.strftime("%Y-%m-%d %H:%M:%S", ltime)

        fileUpdater.debug('the record.db is modified at :'+timeStr)
        cacheTime = currentTime
        if flag:

            data = getTheRealTimeData()

            dataReader.debug('the real-time data is :'+str(data))

            # flagForData = upLoad(data)
            flagForData=upLoadWithoutAG(data)

            if flagForData:
                print 'upload realtime success'
            else:
                print 'failed to upload realtime data ,append it into the failed queue'
                # 历史数据上传失败,实时数据上传失败
                totalFailedLog(data)
                appendIntoFailedQueue(data)
        else:
            print 'the database is not update'

def basisDataUpdater():
    # statinfo = os.stat('/root/JenNet_File/Basis_Data.db')
    # cacheTime = statinfo.st_mtime
    cacheTime = 0.0
    currentTime = 0.0
    index = 2
    sleepTime = 5

    while True:
        flag = False

        for x in range(index):
            statinfo = os.stat('/root/JenNet_File/Basis_Data.db')
            currentTime = statinfo.st_mtime
            if currentTime - cacheTime > 0:
                flag = True
                break
            else:
                time.sleep(sleepTime)

        if cacheTime!=0.0:
            ltime = time.localtime(cacheTime)
            timeStr = time.strftime("%Y-%m-%d %H:%M:%S", ltime)
            basisDataUpdate.debug('the basis.db is modified at :' + timeStr)

        cacheTime = currentTime
        if flag:

            deviceDatas, ChannelDatas,accountDataInfo = getTheBaisData()

            basisDataReader.debug('the deviceInfo is :' + str(deviceDatas))
            basisDataReader.debug('the deviceInfo is :' + str(ChannelDatas))

            flagForDevice = upDate(deviceDatas,deviceUpdateUri)
            flagForChannel = upDate(ChannelDatas,classUpdateUri)
            flagForGateway = upDate(accountDataInfo,gatewayUpdateUri)

            if flagForDevice and flagForChannel and flagForGateway:
                print 'upload realtime success'
            else:
                cacheTime=0.0
                time.sleep(60)
                print 'failed to upload realtime data ,reset the cache time'
        else:
            print 'the database is not update'

def logClear():
    while True:
        time.sleep(259200)
        SqliteUtil.clearLog(failed_post_from_real_records)
        SqliteUtil.clearLog(failed_post_from_failed_records)
        SqliteUtil.clearLog(failed_records)
        SqliteUtil.clearLog(total_records)

def main():
    threadFailed =threading.Thread(target=failedUploader, args=())
    threadReal = threading.Thread(target=realTimeUploader, args=())
    threadBasis = threading.Thread(target=basisDataUpdater,args=())

    threadFailed.start()
    threadReal.start()
    threadBasis.start()



def createDaemon():
    # fork进程
    try:
        if os.fork() > 0: os._exit(0)
    except OSError, error:
        print 'fork #1 failed: %d (%s)' % (error.errno, error.strerror)
        os._exit(1)
    os.chdir('/')
    os.setsid()
    os.umask(0)
    try:
        pid = os.fork()
        if pid > 0:
            print 'Daemon PID %d' % pid
            os._exit(0)
    except OSError, error:
        print 'fork #2 failed: %d (%s)' % (error.errno, error.strerror)
        os._exit(1)
    # 重定向标准IO
    sys.stdout.flush()
    sys.stderr.flush()
    si = file("/dev/null", 'r')
    so = file("/dev/null", 'a+')
    se = file("/dev/null", 'a+', 0)
    os.dup2(si.fileno(), sys.stdin.fileno())
    os.dup2(so.fileno(), sys.stdout.fileno())
    os.dup2(se.fileno(), sys.stderr.fileno())
    # 在子进程中执行代码
    main() # function demo

if __name__ == '__main__':
    t = threading.Thread(target=main, args=())
    t.start()
