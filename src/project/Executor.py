#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ExecutorQueueManager
import random, time, Queue
import time
import ExecutorSocketConnect
import InPutMessage
import OutPutMessage
import SqliteUtil
import logging
import threading
import urllib2
import io
import ConfigParser
import zipfile
import subprocess
import os
import uuid
import platform
import httplib
import json

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

executorSourceId = cf.get('executor','source')
executorPort =cf.get('executor','port')

dbAddr = cf.get('dbdownload','addr')
dbPort =cf.get('dbdownload','port')
dbUri =cf.get('dbdownload','uri')

upAddr = cf.get('update','addr')
upPort =cf.get('update','port')
upUri =cf.get('update','uri')
goalPath = cf.get('update','goalPath')

uploaderPath =cf.get('uploader','path')

regionAddr =  cf.get('region','addr')
regionPort = cf.get('region','port')
regionUri = cf.get('region','uri')



# 日志定义
logger = logging.getLogger("Executor")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("executor.log")

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

# 获取分布式消息中间件 任务队列与响应队列
# task,result =  ExecutorQueueManager.getTaskAndResult()
# 获取与转发网关建立的socket连接
# sock = ExecutorSocketConnect.getSocket()


# 获取本地网关id
def getTheGatewayId():
    # sql = 'SELECT gateway_id from table_device where device_type=\'gateway\''
    sql = 'SELECT gateway_id from table_device'
    result = SqliteUtil.getTheGatewayId(sql)

    # global gatewayId, lock

    # lock.acquire()
    if len(result)==0:
        return ''
    else:
       return result[0]['gateway_id']
    # lock.release()

# 获取与网关连接的socket连接
def getSocket(localGatewayId):

    #随机生成一个本地虚拟网关id
    account_id = localGatewayId[:8]
    fwdGatewayId = account_id+str(uuid.uuid4())[:4]+str(uuid.uuid4())[-4:]

    return ExecutorSocketConnect.getSocket(fwdGatewayId)

# 获取分布式消息中间件的 任务队列与响应队列
def getTaskAndResult():

    task, result = ExecutorQueueManager.getTaskAndResult()
    return task,result

# 日志定义
def getLogger():
    # 日志定义
    logger = logging.getLogger("Executor")
    # 设置日志输出等级
    logger.setLevel(logging.DEBUG)
    # 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
    fh = logging.FileHandler("spam.log")

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

    return logger


# 光强度参数偏差
ILLUMINANCE=12


# 获取设备类型
def getDeviceType(param):

    # 根据入参(参数类型), 差找设备类型
    sql2 = 'SELECT * FROM device_class WHERE main_param=\'' + param + '\''
    deviceClasses = SqliteUtil.getSqliteData(sql2,'')
    device_tpye = deviceClasses[0]['device_type']

    # 根据选定的设备类型,查找可用控制单元
    sql2 = ''

    if device_tpye=='bulb':
        sql2='SELECT * FROM table_region'
    else:
        sql2 = 'SELECT * FROM table_device WHERE device_type=\'' + device_tpye + '\''

    units = SqliteUtil.getSqliteData(sql2,'')
    return deviceClasses[0],units

# 设备值选择(配方参数,传感器数值,当前设备值,参数类型)
def getTheGoalValue(recipe,sensor,currentDeviceValue,param):

    deviceValue=''
    sensorValue=''

    # 根据当前设备值,查找映射出的环境参数
    sql = 'SELECT * FROM map_line WHERE device_value=\''+str(currentDeviceValue)+'\' AND param=\''+param+'\''

    logger.debug('the current device value : '+ str(currentDeviceValue))
    logger.debug('the current recipe value : '+ str(recipe))
    logger.debug('the current sensor value : '+ str(sensor))
    # sql执行过程
    standarSensorValueResult = SqliteUtil.getSqliteData(sql,'')
    # 获取当前设备值对应的环境参数
    standarSensorValue = standarSensorValueResult[0]['sensor_value']
    logger.debug('the current standard device value : '+str(standarSensorValue))

    # 根据当前调控的参数,查找设备所能支持的最大传感器数值
    sqlForLargestSensor = 'SELECT max(sensor_value),device_value FROM map_line WHERE param=\''+param+'\''
    sqlForMinSensor = 'SELECT min(sensor_value),device_value FROM map_line WHERE param=\''+param+'\''

    # sql执行过程
    largestSensorResult = SqliteUtil.getSqliteData(sqlForLargestSensor, '')
    minSensorResult =  SqliteUtil.getSqliteData(sqlForMinSensor, '')

    # 获取当前区域所能支持的最大传感器数值
    maxSensorValue = largestSensorResult[0]['max(sensor_value)']
    # 获取当前区域所能支持的最小传感器数值
    minSensorValue = minSensorResult[0]['min(sensor_value)']


    # 如果配方的数值经过转换后,超过控制单元所能支持的最大传感器数值,该方法返回该设备所能支持的最大值
    if recipe+(standarSensorValue-sensor) >maxSensorValue:
        logger.debug('maxSensorValue = ' +str(maxSensorValue))
        logger.debug('recipe +(standarSensorValue-sensor) = ' +str(recipe+(standarSensorValue-sensor)))
        deviceValue = 255

        logger.debug('the goal device value : ' + str(deviceValue))
        logger.debug('the goal sensor value : ' + str(maxSensorValue))

        return deviceValue
    # 如果配方的数值经过转换后,小于控制单元所能支持的最小传感器数值,该方法返回该设备所能支持的最小值
    if recipe+(standarSensorValue-sensor)<minSensorValue:
        logger.debug('minSensorValue = ' + str(minSensorValue))
        logger.debug('recipe +(standarSensorValue-sensor) = ' + str(recipe + (standarSensorValue - sensor)))

        deviceValue=0

        logger.debug('the goal device value : ' + str(deviceValue))
        logger.debug('the goal sensor value : ' + str(minSensorValue))
        return deviceValue


    # 如果当前传感器数值 与曲线图中的标准传感器数值 的偏差在已定义的可接受范围之内,配方的数值不需要经过处理,直接使用配方的数值,在映射表中查找对应的设备输入值
    if abs(standarSensorValue-sensor) <=ILLUMINANCE:

        # 获取目标值的上边界
        sqlForUpper = 'SELECT min(sensor_value),device_value FROM map_line WHERE sensor_value>=\''+str(recipe)+'\' AND param=\''+param+'\''
        # 获取目标值的下边界
        sqlForFloor = 'SELECT max(sensor_value),device_value FROM map_line WHERE sensor_value<=\''+str(recipe)+'\' AND param=\''+param+'\''
        # 获取目标值的上边界结果
        upperResult = SqliteUtil.getSqliteData(sqlForUpper, '')
        # 获取目标值的下边界结果
        floorResult = SqliteUtil.getSqliteData(sqlForFloor, '')

        logger.debug('the sqlForFloor : '+sqlForFloor)
        logger.debug('the floorResult : '+str(floorResult))
        logger.debug('the sqlForUpper : '+sqlForUpper)
        logger.debug('the upperResult : '+str(upperResult))

        # 如果查询出来的上边界存在,目标设备值与传感器数值 上边界的值就用查询出来的结果
        if upperResult[0]['min(sensor_value)']:
            upperDeviceValue = upperResult[0]['device_value']
            upperSensorValue = upperResult[0]['min(sensor_value)']
            logger.debug('upper is exist')
            logger.debug('upperDeviceValue :' +str(upperDeviceValue))
            logger.debug('upperSensorValue :' +str(upperSensorValue))
        else:
            #如果查询出来的上边界不存在,就使用控制单元所支持的最大值
            upperDeviceValue =largestSensorResult[0]['max(sensor_value)']
            upperSensorValue =largestSensorResult[0]['device_value']
            logger.debug('upper is not exist')
            logger.debug('upperDeviceValue :' + str(upperDeviceValue))
            logger.debug('upperSensorValue :' + str(upperSensorValue))

        # 如果查询出来的下边界存在,目标设备值与传感器数值 下边界的值就用查询出来的结果
        if floorResult[0]['max(sensor_value)']:
            floorDeviceValue = floorResult[0]['device_value']
            floorSensorValue = floorResult[0]['max(sensor_value)']
            logger.debug('floor is exist')
            logger.debug('floorDeviceValue :' + str(floorDeviceValue))
            logger.debug('floorSensorValue :' + str(floorSensorValue))
        else:
            # 如果查询出来的下边界不存在,就使用控制单元所支持的最小值
            floorDeviceValue = minSensorResult[0]['device_value']
            floorSensorValue = minSensorResult[0]['min(sensor_value)']
            logger.debug('floor is not exist')
            logger.debug('floorDeviceValue :' + str(floorDeviceValue))
            logger.debug('floorSensorValue :' + str(floorSensorValue))


        # 如果配方与上边界的差值的绝对值小于配方与下便捷的差值的绝对值, 就使用上边界的值( 数轴上离哪个近,就用哪个),反之依然
        if abs(floorSensorValue-recipe) >abs(upperSensorValue-recipe):

            deviceValue = upperDeviceValue
            sensorValue = upperSensorValue
            logger.debug('abs(floorSensorValue-recipe) >abs(upperSensorValue-recipe)')

        else:
            logger.debug('abs(floorSensorValue-recipe) <=abs(upperSensorValue-recipe):')
            deviceValue = floorDeviceValue
            sensorValue = floorSensorValue
        logger.debug('the goal device value : ' + str(deviceValue))
        logger.debug('the goal sensor value : ' + str(sensorValue))

    # 如果当前传感器数值 与曲线图中的标准传感器数值 的偏差超过已定义的可接受范围,配方的数值需要经过处理,使用经过处理的数值,在映射表中查找对应的设备输入值
    else:

        # 查询矫正后的配方数值,在硬件能里曲线中的上下边界
        sqlForUpper = 'SELECT min(sensor_value),device_value FROM map_line WHERE sensor_value>=\'' + str(recipe+(standarSensorValue-sensor)) + '\' AND param=\'' + param + '\''
        sqlForFloor = 'SELECT max(sensor_value),device_value FROM map_line WHERE sensor_value<=\'' + str(recipe+(standarSensorValue-sensor)) + '\' AND param=\'' + param + '\''
        upperResult = SqliteUtil.getSqliteData(sqlForUpper, '')
        floorResult = SqliteUtil.getSqliteData(sqlForFloor, '')


        # logger.debug('the sqlForFloor : '+sqlForFloor)
        # logger.debug('the floorResult : '+str(floorResult))
        # logger.debug('the sqlForUpper : '+sqlForUpper)
        # logger.debug('the upperResult : '+str(upperResult))

        # 如果查询出来的上边界存在,目标设备值与传感器数值 上边界的值就用查询出来的结果
        if upperResult[0]['min(sensor_value)']:
            upperDeviceValue = upperResult[0]['device_value']
            upperSensorValue = upperResult[0]['min(sensor_value)']
            logger.debug('upper is exist')
            logger.debug('upperDeviceValue :' + str(upperDeviceValue))
            logger.debug('upperSensorValue :' + str(upperSensorValue))
        else:
            # 如果查询出来的上边界不存在,就使用控制单元所支持的最大值
            upperSensorValue= largestSensorResult[0]['max(sensor_value)']
            upperDeviceValue = largestSensorResult[0]['device_value']
            logger.debug('upper is not exist')
            logger.debug('upperDeviceValue :' + str(upperDeviceValue))
            logger.debug('upperSensorValue :' + str(upperSensorValue))

        # 如果查询出来的下边界存在,目标设备值与传感器数值 下边界的值就用查询出来的结果
        if floorResult[0]['max(sensor_value)']:
            floorDeviceValue = floorResult[0]['device_value']
            floorSensorValue = floorResult[0]['max(sensor_value)']
            logger.debug('floor is exist')
            logger.debug('floorDeviceValue :' + str(floorDeviceValue))
            logger.debug('floorSensorValue :' + str(floorSensorValue))
        else:
            # 如果查询出来的下边界不存在,就使用控制单元所支持的最小值
            floorDeviceValue = minSensorResult[0]['device_value']
            floorSensorValue = minSensorResult[0]['min(sensor_value)']
            logger.debug('floor is not exist')
            logger.debug('floorDeviceValue :' + str(floorDeviceValue))
            logger.debug('floorSensorValue :' + str(floorSensorValue))

        # 如果配方与上边界的差值的绝对值小于配方与下便捷的差值的绝对值, 就使用上边界的值( 数轴上离哪个近,就用哪个),反之依然
        if abs(floorSensorValue - recipe) > abs(upperSensorValue - recipe):
            deviceValue = upperDeviceValue
            sensorValue = upperSensorValue
            logger.debug('abs(floorSensorValue-recipe) >abs(upperSensorValue-recipe)')
        else:
            deviceValue = floorDeviceValue
            sensorValue = floorSensorValue
            logger.debug('abs(floorSensorValue-recipe) <=abs(upperSensorValue-recipe)')

    logger.debug('the goal device value : '+ str(deviceValue))
    logger.debug('the goal sensor value : '+ str(sensorValue))

    return deviceValue

# 获取当前设备值
def getCurrentDeviceValue(units):
    logger.debug('the controller is : '+str(units))

    try:
        origionalValue = units[0]['device_value']
        deviceGuid = units[0]['device_guid']
        deviceAddr = units[0]['device_addr']
        gatewayId = units[0]['gateway_id']
    except Exception ,e:
        origionalValue = units[0]['region_value']
        deviceGuid = units[0]['region_guid']
        deviceAddr = units[0]['region_addr']
        gatewayId = units[0]['gateway_id']
        print e

    # 获取设备值的字符串长度
    origionaLength = len(origionalValue)

    origionalValues = {}
    origionalValuesLen = 0

    # 如果设备值的字符串长度不足2 就将目标长度值置为1
    if (origionaLength / 2) == 0:
        origionalValuesLen = 1
    else:
        # 如果设备值的长度大于2 , 就将目标长度置为设备值的长度的一半
        origionalValuesLen = origionaLength / 2

    # 类似for (int i=0;i<m;i++)
    for index in range(origionalValuesLen):
        # 将16进制字符串转换为int整数
        tempHexValue = origionalValue[index * 2:index * 2 + 2]
        tempIntValue = int(tempHexValue, 16)

        if tempIntValue == 0:
            # 为了避免出现关闭设备的情况,当设备的某个通道的值为0的时候,默认将这个通道值改为1
            tempIntValue = 1
        # 将经过转换的设备值写入数组中
        origionalValues[index] = tempIntValue

    # 返回设备值的数组的同时,将设备值的擦痕高难度,主键,地址,网关id一并返回
    return origionalValues, origionalValuesLen,deviceGuid,deviceAddr,gatewayId

# 组装拼接设备值
def getGoalValue(origionalValuesLen,goalValue,deviceValue,origionalValues):

    # 将存储与数组中的10进制设备值转化为16进制字符串
    for index in range(origionalValuesLen):

        finnalIntvalue = int(goalValue)
        if finnalIntvalue > 255:
            finnalIntvalue = 255

        if finnalIntvalue < 16:
            origionalValues[index] = '0' + hex(finnalIntvalue)[2:]
        else:
            origionalValues[index] = hex(finnalIntvalue)[2:]

        deviceValue = deviceValue + origionalValues[index]

    return deviceValue

#  报告设备异常
def recipeStatusUploader(regionStaus):

    result = True

    # 将区域状态参数转化为一个json对象
    params = json.dumps(regionStaus)

    print 'update the status of the recipe'
    print params

    # 请求头设置
    headers = {"Content-type": "application/json", "Accept": "*/*"}

    httpClient = None

    try:

        # 设置http客户端, 地址/端口/超时时长
        httpClient = httplib.HTTPConnection(regionAddr, int(regionPort), timeout=3)
        # 设置请求 method/uri/body/header 并发送请求
        httpClient.request("POST", regionUri, params, headers)

        # 获取请求的响应
        response = httpClient.getresponse()

        # 异常判断
        if response.status != 200:
            result = False

        strResult = response.read()

        jsonReslut = json.loads(strResult)
        print jsonReslut

        if jsonReslut['code'] != '0':
            result = False
            if jsonReslut['message'] == 'DuplicateKeyException':
                return result

        return result

    except Exception:
        result = False
    finally:
        if httpClient != None:
            httpClient.close()
            return result

# 发送控制指令至子网关
def controll(deviceValue,deviceAddr,sock,gatewayId,units):

    # 获取控制单元的第一项,用于验证该控制单元的类型
    unit = units[0]


    unitType=''
    # 如果其中的字段包含 region ,  说明该控制单元是一个区域
    for column in unit:
        if 'region' in column:
            unitType='region'
            break


    logger.debug('the units input is : ' + deviceValue)

    controlSql = ''

    # 如果控制单元是一个区域,这里的sql就使用区域来完成
    if unitType=='region':
        controlSql='UPDATE table_region SET region_value = \'' + deviceValue + '\' , region_delay=\'1\'  WHERE region_addr=\'' + deviceAddr + '\''
    else:
        controlSql='UPDATE table_device SET device_value = \'' + deviceValue + '\' , device_delay=\'1\'  WHERE device_addr=\'' + deviceAddr + '\''

    # 设置用于转发网络的控制消息
    controll = {}
    controll['Command'] = controlSql
    controll['DestinationID'] = gatewayId
    controll['List'] = []
    controll['Message'] = "table_device"
    controll['PackageNumber'] = '1874'
    controll['SourceID'] = executorSourceId
    controll['Status'] = 0
    controll['Type'] = "NULL"

    logger.debug('Sending the command to the gateway : ' + str(controll))

    # 将json数组转化为二进制数据
    outputData = OutPutMessage.getBytes(controll)
    json = ""

    # 尝试3次控制,只要成功一次,就结束循环
    for i in range(3):

        # 每次等待2秒
        time.sleep(2)

        # 发送二进制数据
        sock.send(outputData)

        # 接收来响应
        data = sock.recv(2048)
        bytes = bytearray(data)

        # 将响应数据转化为json
        json = InPutMessage.getJson(bytes)

        logger.debug('The command recived from gateway is : '+ str(json))
        # 如果控制成功,跳出循环
        if json['Status'] == 0:
            logger.debug('the command executed successfully after tried %d times'%(i+1))
            break
    # 如果3次控制都失败,此时的json数据一定是一个失败的
    if json['Status'] != 0:
        logger.debug('the command executed failed in %d times'%(3))

    return json

# 更新设备表中的设备值(recipe.db)
def updateData(json,deviceValue,deviceGuid):

    logger.debug('start the process to update the device_value')

    if json['Status'] == 0:

        logger.debug('command has been executed successfully!')

        sql3 = 'update table_region set region_value = \'' + deviceValue + '\' where region_guid=\'' + deviceGuid + '\''
        logger.debug(sql3)
        count = SqliteUtil.updateSqlData(sql3)

        if count < 1:
            logger.debug('failed to update the device value : %s', sql3)
        else:
            logger.debug('update the device value successfully!')
    else:
        logger.debug('gateway can not execute the command : ' + str(json))


# 控制
def Listener(recivedParam,result,sock):

    #1 解析任务队列信息
    recipe = float(recivedParam[1])
    sensor = float(recivedParam[2])
    param = recivedParam[0]

    logger.debug('star the controller')

    logger.debug('recipe data : %s ,sensor data : %s ,param data: %s'%(recivedParam))


    deviceType = ''

    #2 根据控制的环境参数,获取控制单元
    deviceClass, units = getDeviceType(param)

    logger.debug('the controll units : ' + str(units))

    deviceValue = ''

    #3 根据控制单元目前的设备值,生成可计算的设备值/输入长度, 并返回单元主键,单元地址,单元网关
    origionalValues, origionalValuesLen, deviceGuid, deviceAddr,gatewayId = getCurrentDeviceValue(units)

    #4  根据传感器数值与配方数值,计算出最终的单元控制目标数值
    goalValue = getTheGoalValue(recipe, sensor, origionalValues[0], param)

    #5 将第四部得出的计算表达式,生成字符串型的设备输入值
    deviceValue = getGoalValue(origionalValuesLen, goalValue, deviceValue,origionalValues)

    #6 对控制单元发起控制动作
    jsonResult = controll(deviceValue, deviceAddr,sock,gatewayId,units)

    #7 上报控制结果
    regionStatus={}

    if jsonResult['Status'] != 0:
        regionStatus['region_guid']=units[0]['region_guid']
        regionStatus['region_status']='1'
    else:
        regionStatus['region_guid'] = units[0]['region_guid']
        regionStatus['region_status'] = '0'
    recipeStatusUploader(regionStatus)

    # 更新控制单元数据(recipe.db)
    updateData(jsonResult,deviceValue,deviceGuid)
    result.put(jsonResult)

# 下载配方数据文件(recipe.db)
def downloadFile(user_id,region_id,recipe_id):

    logger.debug('start the download process')

    # 格式化输出url
    url = 'http://%s:%s%s?user_id=%s&region_id=%s&recipe_id=%s'% (dbAddr,dbPort,dbUri,user_id, region_id,recipe_id)
    logger.debug('the download url:'+url)


    # get方法直接获取配方数据
    response = urllib2.urlopen(url)
    html = response.read()

    # 将配方db文件放置在与上级目录同级的位置
    file = io.open('../recipe.db', 'wb')

    file.write(html)
    file.flush()
    file.close()

    logger.debug('downloaded the db file')


# 固件程序下载
def downloadPragram():
    logger.debug('start the update process')
    url = 'http://%s:%s%s' % (upAddr, upPort, upUri)
    logger.debug('the update url:' + url)

    response = urllib2.urlopen(url)
    html = response.read()

    file = io.open('temp.zip', 'wb')

    file.write(html)
    file.flush()
    file.close()
    logger.debug('downloaded the program packages')
# 解压固件程序
def extract():
    f = zipfile.ZipFile('temp.zip', 'r', zipfile.ZIP_DEFLATED)
    f.extractall(goalPath)
    f.close()
    logger.debug('extract the program packages successfully')

    if os.path.exists('temp.zip'):
        os.remove('temp.zip')
    logger.debug('clear the temp file successfully')


def main(task, result, localGatewayId):
    sock = getSocket(localGatewayId)
    # task, result = getTaskAndResult()
    while True:

     try:
      time.sleep(5)
      recivedParam = task.get(True)

      print recivedParam

      if recivedParam[0]=='download':

          message = recivedParam[2]

          messageList = message.split(',')

          userId = messageList[0]
          regionId = messageList[1]
          recipeId = messageList[3]

          downloadFile(userId,regionId,recipeId)
          result.put('done')

      if recivedParam[0]=='illuminance':
          t=threading.Thread(target=Listener,args=(recivedParam,result, sock))

          result.put('recipe start')
          t.start()

      if recivedParam[0]=='update':

          downloadPragram()
          extract()
          result.put('ready to reboot in 5s')
          time.sleep(2)
          subprocess.call(['cp','/root/JenNet_File/sonneteckAutoController/src/project/config.conf','/sbin/'])
          subprocess.call(['cp', '/root/JenNet_File/sonneteckAutoController/src/project/SensorDataUploader.py','/sbin/'])
          subprocess.call(['cp', '/root/JenNet_File/sonneteckAutoController/src/project/SqliteUtil.py', '/sbin/'])
          subprocess.call(['reboot'])

     except Exception,e:
         try:
          task, result = getTaskAndResult()
          localGatewayId = getTheGatewayId()
         except Exception:
             print e.message
         logger.error(e)



if __name__ == '__main__':
    task, result=getTaskAndResult()
    gateway_id= getTheGatewayId()
    main(task,result,gateway_id)