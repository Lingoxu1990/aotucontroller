#!/usr/bin/env python
# -*- coding: utf-8 -*-

import SqliteUtil
import logging
import ConfigParser
import OutPutMessage
import time
import InPutMessage
import SensorDataProducer
import uuid
import DispatcherSocketConnect
import platform


cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

executorSourceId = cf.get('dispatcher1','source')
executorPort =cf.get('dispatcher1','port')

# 日志定义
logger = logging.getLogger("Init")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("Init.log")

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

# 将输出起增加至logger(executor)
logger.addHandler(ch)
logger.addHandler(fh)


def bulbInit(regionId,sock,fwdGatway):

    region, gateways = regionSearcher(regionId)

    failedMessages = initRegions(region, gateways, sock,fwdGatway)


# 控制区域
def controll(initValue,regionAddr,sock,gatewayId,fwdGatway):

    controll = {}
    controll['Command'] = 'UPDATE table_region SET region_value = \'%s\', region_delay=\'1\' WHERE region_addr=\'%s\'' % (initValue,regionAddr)
    controll['DestinationID'] = gatewayId
    controll['List'] = []
    controll['Message'] = "table_region"
    controll['PackageNumber'] = '1874'
    controll['SourceID'] = fwdGatway
    controll['Status'] = 0
    controll['Type'] = "NULL"

    logger.debug('Sending the command to the gateway : ' + str(controll))
    print 'Sending the command to the gateway : ' + str(controll)

    outputData = OutPutMessage.getBytes(controll)
    json = ""
    for i in range(6):
        time.sleep(1)
        sock.send(outputData)
        data = sock.recv(2048)
        bytes = bytearray(data)
        json = InPutMessage.getJson(bytes)
        logger.debug('The command recived from gateway is : '+ str(json))
        print 'The command recived from gateway is : '+ str(json)

        if json['Status'] == 0:
            logger.debug('the command executed successfully after tried %d times'%(i+1))
            break


    if json['Status'] != 0:
        logger.debug('the command executed failed in %d times'%(6))

    return json


def regionSearcher(regionId):

    sqlForRegionValue = 'SELECT * from table_region WHERE region_guid=\'%s\'' % (regionId)

    regions = SqliteUtil.getSqliteData(sqlForRegionValue, '')
    logger.debug('the region info :' +str(regions))

    sqlForRegionGateway = 'SELECT gateway_id FROM table_region_device WHERE region_guid=\'%s\' GROUP BY gateway_id' % (regionId)

    gateways = SqliteUtil.getSqliteData(sqlForRegionGateway, '')
    print gateways

    logger.debug('the region gateway are : ' + str(gateways))

    return regions[0],gateways

def getRegionSensors(regionId):

    sql = 'SELECT device_guid,gateway_id FROM table_device WHERE device_guid IN(SELECT table_device_guid FROM  table_region_device WHERE region_guid = \'%s\' GROUP BY table_device_guid) and device_type=\'sensor\''%(regionId)

    sensors = SqliteUtil.getSqliteData(sql, '')

    logger.debug('the sensor in the region : '+str(sensors))
    return sensors

def mapLine(list):

    SqliteUtil.Insert(list)

def initRegions(region,gateways,sock,fwdGatway):

    failedMessages=[]

    regionValue = region['region_value']
    regionAddr = region['region_addr']

    logger.debug('the region value : %s'% regionValue)
    logger.debug('the region addr : %s' % regionAddr)


    print regionValue,regionAddr

    origionaLength = len(regionValue)

    if (origionaLength / 2) == 0:
        origionalValuesLen = 1
    else:
        origionalValuesLen = origionaLength / 2

    mapList=[]



    for valueIndex in range(256):

        param = []
        param.append(str(uuid.uuid1()))
        param.append('illuminance')

        initValue = ''
        for lenIndex in range(origionalValuesLen):
            if valueIndex < 16:
                initValue = initValue + '0' + hex(valueIndex)[2:]
            else:
                initValue = initValue + hex(valueIndex)[2:]

        for gateway in gateways:
            print gateway['gateway_id']
            controlResult = controll(initValue, regionAddr, sock, gateway['gateway_id'],fwdGatway)

            sensors = getRegionSensors(region['region_guid'])

            if controlResult['Status'] != 0:
               failedMessage={}
               failedMessage['init_value']=str(initValue)
               failedMessage['gateway_id']=gateway['gateway_id']
               failedMessage['region_guid']=region['region_guid']
               failedMessage['region_addr']=region['region_addr']
               failedMessages.append(failedMessage)
               param.append('')
               param.append(str(valueIndex))
            else:
               sensorData = SensorDataProducer.produce(sock, sensors,fwdGatway)
               param.append(sensorData['illuminance'])
               param.append(str(valueIndex))

        time.sleep(1)

        print 'the mapping :' +str(param)
        logger.debug('the mapping :' +str(param))

        mapList.append(tuple(param))

    mapLine(mapList)

    return failedMessages

def main():
    sock = DispatcherSocketConnect.getFwdSocket()
    # bulbInit('d6e20855-d14d-40ff-a7f6-533caffc256b', sock)


# main()