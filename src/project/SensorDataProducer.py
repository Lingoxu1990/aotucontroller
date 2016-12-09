#!/usr/bin/env python
# -*- coding: utf-8 -*-
import InPutMessage
import OutPutMessage
import sqlite3
import ConfigParser
import logging
import datetime
import platform
cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

dispatcherSourceId = cf.get('dispatcher','source')
dispatcherPort =cf.get('dispatcher','port')

dispatcher1SourceId = cf.get('dispatcher1','source')
dispatcher1Port =cf.get('dispatcher1','port')


# 日志定义
logger = logging.getLogger("SensorDataProducer")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("SensorDataProducer.log")

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


def produce(sock,sensors,fwdGateway):


    subResult=[]
    for sensor  in sensors:
        gatewayId = sensor['gateway_id']

        sql = 'SELECT * FROM table_sensor_record WHERE record_time = (SELECT max(datetime(record_time)) FROM table_sensor_record WHERE table_device_guid=\''+sensor['device_guid']+'\' ORDER BY datetime(record_time))'

        socketMessage = {}
        socketMessage['Command'] = sql
        socketMessage['DestinationID'] = gatewayId
        socketMessage['List'] = []
        socketMessage['Message'] = 'getSensor'
        socketMessage['PackageNumber'] = '1874'
        socketMessage['SourceID'] = fwdGateway
        socketMessage['Status'] = 1
        socketMessage['Type'] = 'table_sensor_record'

        logger.debug('the request content send to the gateway%s was : '%(gatewayId)+str(socketMessage))

        outPutData= OutPutMessage.getBytes(socketMessage)
        sock.send(outPutData)

        logger.debug('the request was send at :' +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        data=sock.recv(2048)

        logger.debug('the response was out of time :' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        bytes = bytearray(data)

        logger.debug('the response was received at :' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        json = InPutMessage.getJson(bytes)

        logger.debug('the response content received was : ' + str(json))

        subResult.append(json['List'][0])

    print subResult

    air_temperature = 0.0
    air_humidity = 0.0
    soil_temperature = 0.0
    soil_PH_value = 0.0
    soil_humidity = 0.0
    carbon_dioxide = 0.0
    illuminance = 0.0
    soil_conductivity = 0.0
    photons = 0.0
    liquid_PH_value = 0.0
    lai_value = 0.0

    result={}

    for record in subResult:
        air_temperature = air_temperature+float(record['air_temperature'])
        air_humidity =air_humidity+ float(record['air_humidity'])
        soil_temperature= soil_temperature + float(record['substrate_temperature'])
        soil_PH_value = soil_PH_value +float(record['substrate_ph'])
        soil_humidity = soil_humidity +float(record['substrate_humidity'])
        carbon_dioxide = carbon_dioxide + float(record['carbon_dioxide'])
        illuminance = illuminance +float(record['illuminance'])
        soil_conductivity = soil_conductivity +float(record['substrate_conductivity'])
        photons = photons + float(record['ppfd'])
        liquid_PH_value =liquid_PH_value + float(record['liquid_ph'])
        # lai_value = lai_value +float(record['lai'])

    result['air_temperature']=air_temperature
    result['air_humidity']=air_humidity
    result['substrate_temperature']=soil_temperature
    result['substrate_ph']=soil_PH_value
    result['substrate_humidity']=soil_humidity
    result['carbon_dioxide']=carbon_dioxide
    result['illuminance']=illuminance
    result['substrate_conductivity']=soil_conductivity
    result['ppfd']=photons
    result['liquid_ph']=liquid_PH_value
    result['lai']=lai_value

    logger.debug('the average value of the sensors is : ' + str(result))

    return result














