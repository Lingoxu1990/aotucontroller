#!/usr/bin/env python
# -*- coding: utf-8 -*-

import RecipeDataProducer
import SensorDataProducer
import DispatcherQueueManager
import time
import SqliteUtil
import logging
import DispatcherSocketConnect
import InPutMessage
import OutPutMessage
import threading
import subprocess
import Initer
import json
import httplib
import ConfigParser
import uuid
import platform
import traceback
import hashlib
import urllib
import inspect
import ctypes
import Queue


# 获取配置文件资源读取类
cf = ConfigParser.ConfigParser()

# 不同环境下,读取不同的配置文件
osPlatporm = platform.system()

if osPlatporm == 'Linux':
    # 在子网关的环境下,读取/sbin/config.conf
    cf.read("/sbin/config.conf")
else:
    # 在非子网关环境下,读取同级目录下的配置文件
    cf.read("./config.conf")

# 调度进程位置汇报所需要的外部配置
# serverAddr 服务器地址
serverAddr = cf.get('dispatcherLocation','addr')
# serverAddr 服务器端口
serverPort =cf.get('dispatcherLocation','port')
# serverAddr 接口url
serverUri = cf.get('dispatcherLocation','uri')

# AG相关参数获取
appId = cf.get('AG','appId')
token = cf.get('AG','token')
# AG获取时间戳的url
timestamp = cf.get('AG','timestamp')
# AG的md5加密参数(false 的替换体)
md5Param = cf.get('AG','md5Param')

# 配方运行状态告警接口
# 端口
recipePort =cf.get('recipe','port')
# url
recipeUri = cf.get('recipe','uri')



# 日志定义
logger = logging.getLogger("Dispatcher")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("dispatcher.log")

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


# 获取网关id(数据源 子网关的设备表)
# 返回类型为字符串
def getTheGatewayId():

    sql = 'SELECT gateway_id from table_device'
    result = SqliteUtil.getTheGatewayId(sql)

    if len(result)==0:
        return ''
    else:
       return result[0]['gateway_id']



# 中断线程方法(线程id+系统异常)
def _async_raise(tid, exctype):
    tid = ctypes.c_long(tid)
    if not inspect.isclass(exctype):
        exctype = type(exctype)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, ctypes.py_object(exctype))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(tid, None)
        raise SystemError("PyThreadState_SetAsyncExc failed")

# 中断线程方法(线程对象)
def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)

# 获取调度进程与转发网关的socket连接
def getSocket(dispatcherGateway,fwdGateway):
    # 获取一个socket连接 ,用于保持与web服务器的通信(该socket连接在转发网关的位置,将汇报至web服务器,所以web服务器能够找到该socket连接)
    websocket = DispatcherSocketConnect.getWebSocket(dispatcherGateway)
    # 获取一个socket连接, 仅用于保持与转发网络的连接(用于获取实时的传感器数据)
    fwdsocket = DispatcherSocketConnect.getFwdSocket(fwdGateway)

    return websocket,fwdsocket

# 初始化区域 传入参数为区域id, 一个用于保持与转发网络通信的socket实例
def InitRegion(regionId,sock,fwdGatway):
    # 调用区域初始化方法
    Initer.bulbInit(regionId,sock,fwdGatway)

    # # 用于演示配方的标准用例
    # mapList=[]
    # for x in range(256):
    #     param=[]
    #     param.append(str(uuid.uuid1()))
    #     param.append('illuminance')
    #     param.append(str(x*30))
    #     param.append(x)
    #     mapList.append(tuple(param))
    # # 将预设的区域硬件能力写入 recipe.db
    # Initer.mapLine(mapList)

# 获取环境参数与配方参数 , 通过消息队列下发至 控制进程
def feedback(queueList,messageList,socket,fwdGateway):
    # 测试用例1 配方主动变化, 环境参数随之发生变化
    testIntance=[
        (7650,6660),
        (6660,5700),
        (5700,4800),
        (4800,4200),
        (4200,3000),
        (3000,1800),
        (1800,0),
        (0,1800),
        (1800,3000),
        (3000,4200),
        (4200,4800),
        (4800,5700),
        (5700,6660),
        (6660,7650)
    ]
    # 测试用例2 外界增加干扰,控制程序不断调整环境参数的一个过程
    testIntance3=[
        (7650,7650),
        (9450,7650),
        (7650,7650),
        (9450,7650),
        (7650,7650),
        (9450,7650),
        (7650,7650),
        (9450,7650),
        (7650,7650),
        (5850,7650),
        (7650,7650),
        (5850,7650),
        (7650,7650),
        (5850,7650),
        (7650,7650),
        (5850,7650),
        (7650,7650)
    ]

    # 保存 配方数据与环境数据的 异常情况,如果异常,数据为1 ,正常为0
    # 例: [0,1,0,1,1,1]
    errors=[]

    # 测试用例轮询 使用到的计数器
    index=0

    while True:
        # 每10秒调用一次
        time.sleep(10)

        # 轮询测试用例数据
        seq=index%len(testIntance)
        recipeIll=testIntance[seq][1]
        senIll=testIntance[seq][0]
        index=index+1

        # 异常的次数
        errorValue=0
        # 轮询配方运行的异常状态list
        for error in errors:
            # 累加异常次数
            errorValue=errorValue+error

        # 如果 异常数超过5次 说明配方的运行出现问题
        if errorValue>5:
            # 用于上传的配方运行状态json数据
            privateRecipeIndex={}
            # 配方id
            privateRecipeIndex['private_recipe_id']=messageList[3]
            # 配方状态 2表示配方在异常状态下运行
            privateRecipeIndex['status']='2'
            # 区域id
            privateRecipeIndex['region_guid']=messageList[1]
            # 上传配方的运行状态
            recipeStatusUploader(privateRecipeIndex)
        else:

            # 上传的配方运行状态json数据
            privateRecipeIndex = {}
            # 配方id
            privateRecipeIndex['private_recipe_id'] = messageList[3]
            # 配方状态 1表示配方在正常状态下运行
            privateRecipeIndex['status'] = '1'
            # 区域id
            privateRecipeIndex['region_guid'] = messageList[1]
            # 上传配方的运行状态
            recipeStatusUploader(privateRecipeIndex)


        # 获取配方的数据(当前时刻)
        recipeData = RecipeDataProducer.produce()


        print recipeData

        if len(recipeData)==0:
            logger.error('there is no recipe Data')
            # 如果无法获取当前的配方数据,本次循环跳过
            continue


        # 获取当前区域的所有传感器
        sensors = getTheRegionDeivceOfTheRecipe(messageList[3])

        if len(sensors)==0:
            logger.error('there is no sensor in the region')
            # 如果当前区域没有传感器,本次循环跳过
            continue

        # 获取区域传感器数据(已做均值处理)
        sensorData = SensorDataProducer.produce(socket, sensors,fwdGateway)



        if abs(float(recipeData['illuminance'][0]) -float(sensorData['illuminance']))>100:
            # 如果传感器数据与配方数据的差值超过100,认为此时的配方运行存在异常

            if len(errors)<10:
                # 如果异常列表的长度小于10 , 直接在异常列表中增加一个异常数据
                errors.append(1)
            else:
                # 如果异常列表的长度大于或等于10 , 剔除列表第一项,在尾部增加一个异常数据
                errors = errors[1:10]
                errors.append(1)
        else:
            # 如果传感器数据与配方数据的差值小于或等于100,认为此时的配方运行正常
            if len(errors) < 10:
                # 如果异常列表的长度小于10 , 直接在异常列表中增加一个正常常数据
                errors.append(0)
            else:
                # 如果异常列表的长度大于或等于10 , 剔除列表第一项,在尾部增加一个正常数据
                errors = errors[1:10]
                errors.append(0)

        # 遍历 通信队列,发送控制消息元组
        for gatewayId, task_queue, result_queue in queueList:

            # 消息格式是一个三元组 ('参数名称','配方数值','传感器数值') 配方数值与传感器数值要转化为 字符串形式
            task = ('illuminance', recipeData['illuminance'][0], str(sensorData['illuminance']))
            # task = ('illuminance', str(recipeIll), str(senIll))
            logger.debug('Message send to the executor is :' + str(task))

            # 通过消息队列发送消息
            task_queue.put(task)
            # 接受响应队列的消息 最多阻塞2秒
            result_queue.get(2)



# 获取消息队列
def getTaskAndResult():

    task, result = DispatcherQueueManager.getTaskAndResult()
    return task, result

# 根据配方主键,查找传感器(recipe.db)
def getTheRegionDeivceOfTheRecipe(recipe_id):

    sql = 'select device_guid,gateway_id from table_device where device_guid in (SELECT table_device_guid FROM table_region_device where region_guid=(select region_guid from private_recipe_index where private_recipe_id = \''+recipe_id+'\') GROUP BY table_device_guid) and device_type=\'sensor\''

    sensors = SqliteUtil.getSqliteData(sql, '')

    logger.debug('the sql for query the sensors in the region is :'+sql)

    return sensors

# 获取ag的sign
def agSign():
    param={}
    httpClient = None

    try:
        headers = {"Accept": "application/json"}

        # 设置http连接的地址,端口,超时长度
        httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
        # 设置请求的方法/GET,uri ,请求体/'',请求头/headers , 并发送请求
        httpClient.request("GET", timestamp, '', headers)

        # 接受请求响应
        response = httpClient.getresponse()

        # 获取请求响应数据
        tms = response.read()

        # 将请求响应数的字符串据转化为json对象
        tms = json.loads(tms)
        # 获取响应内容 ag返回的时间戳
        ts = tms['content']
        # 获取md5转换工具累
        md5Eencoder = hashlib.md5()

        # 拼接等待md5加密的参数
        aaa = appId + str(ts) + token + md5Param

        # 将字符串写入转换工具类中
        md5Eencoder.update(aaa)
        # 输出加密后的sign
        sign = md5Eencoder.hexdigest()

        # 组装调用ag所需要的参数
        param = {'sign': sign, 'ts': ts, 'appID': appId}

        # 将map转换为 x=x%y=y的格式
        param = urllib.urlencode(param)
        # 将uri参数返回
        return param

    except Exception,e:
        print e
    finally:
        if httpClient != None:
            httpClient.close()
            return  param

# 上传调度进程所在的位置
def locationUploader(params):

    # 获取调用ag 所需要的uri参数
    param = agSign()
    # print params
    # 本次上传是否成功的标志位,最终将做为参数返回给调用者
    result=True
    # 将调度进程信息的键值对 转化为json对象
    params = json.dumps(params)
    # 设置http请求头
    headers = {"Content-type": "application/json", "Accept": "*/*"}
    # http对象
    httpClient=None

    print '%s?%s'%(serverUri,param)
    print params
    print serverAddr
    print serverPort

    try:

     #  设置http连接的地址,端口,超时长度
     httpClient = httplib.HTTPConnection(serverAddr, int(serverPort), timeout=3)
     # 设置请求的参数 method /uri / body/header ,并发送请求
     httpClient.request("POST", '%s?%s'%(serverUri,param), params, headers)

     # 获取请求响应
     response = httpClient.getresponse()

     if response.status!=200:
         # 如果接口异常,返回失败
         result=False
     # 获取响应的数据(字符串)
     strResult = response.read()

     # 将字符串转换为json对象
     jsonReslut = json.loads(strResult)

     print jsonReslut
     #   如果接口 响应失败, 返回失败信息
     if jsonReslut['code']!='0':
         result=False
         if jsonReslut['message']=='DuplicateKeyException':
             return result

     return  result

    except Exception:
        result=False
    finally:

        # 关闭http连接
        if httpClient!=None:
          httpClient.close()
          return result

#  根据节点列表,以及调度进程的网关id, 转化web服务器接口可用的数据格式
def localtionProducer(nodeList,leader):

    # leader 是指调度进程所在的网关 的网关id
    result=[]

    for gateway,addr in nodeList:
        temp={}
        temp['id']=str(uuid.uuid4())
        temp['account_id']=gateway[0:8]
        temp['dispatcher_gateway']=leader
        temp['sub_gateway']=gateway
        result.append(temp)

    return result
# 调度进程根据通信队列列表, 发送下载命令
def downloadCommand(queueList,user_id,region_id,recipe_id):

    # 轮询通信队列,发送下载命令
    for gatewayId, task_queue, result_queue in queueList:
        task_queue.put(('download','','%s,%s,%s,%s'%(user_id,region_id,'',recipe_id)))
        print result_queue.get(1)

# 配方状态上传
def recipeStatusUploader(recipeIndex):

    result = True


    # 将配方状态数据转化为json 对象
    params = json.dumps(recipeIndex)

    print 'update the status of the recipe'
    print params

    headers = {"Content-type": "application/json", "Accept": "*/*"}

    httpClient = None


    try:

        # 设置http连接的 地址,端口,超时时常
        httpClient = httplib.HTTPConnection(serverAddr, int(recipePort), timeout=3)
        # 设置请求的method/uri/body/headers
        httpClient.request("POST", recipeUri, params, headers)

        # 获取http 响应
        response = httpClient.getresponse()

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

def test():

    task, result = DispatcherQueueManager.getTaskAndResult()

    recipeData = 3000
    while True:
        feedback(task, result, str(recipeData), str(recipeData-500))

        recipeData = recipeData=recipeData-500

        if recipeData<0:
            break


# 主方法入口
# queueList 消息队列的列表
# nodeList  节点列表
# leader    调度进程所在节点的网关id
def main(queueList,nodeList,leader):
    # 用于存储配方反馈程序的线程对象
    threadControl = []
    # 根据网关ID前8位,获取用户id
    account_id = leader[0:8]
    # 随机生成调度进程在转发网络中的位置id(虚拟的一个gateway_id),该id将被上传至服务器,让服务器下发指令
    dispatcherGateway = account_id + str(uuid.uuid4())[:4] + str(uuid.uuid4())[-4:]
    fwdGateway=''
    while True:
        #  随机生成调度进程与转发网关的另一个连接id,该id只要不与 dispatcherGateway 相同就行。
        fwdGateway = account_id + str(uuid.uuid4())[:4] + str(uuid.uuid4())[-4:]
        if dispatcherGateway != fwdGateway:
            break

    # 根据2个虚拟位置,生成2个与转发网关连接的socket
    # websocket  用于接受web服务器下发的指令,并对其响应
    # fwdsocket  用于本地与转发网关的通信
    websocket, fwdsocket = getSocket(dispatcherGateway,fwdGateway)

    # task, result=getTaskAndResult()

    # 根据消息队列的列表,节点列表 生成转发网关的位置信息(包含了调度进程的管辖范围)
    locations = localtionProducer(nodeList,dispatcherGateway)
    # 上传位置信息
    locationUploader(locations)

    while True:
      try:

        # 接收socket传入的二进制数据,长度设置为4000
        data = websocket.recv(4000)
        if len(data)<8:
            # 排除传输的数据量小于8字节的情况
            continue
        print data
        bytes = bytearray(data)

        # 将二进制数据转化为json对象
        json = InPutMessage.getJson(data)

        logger.debug('receive :'+str(json))

        # 获取下发的消息(message 用于存储字符串命令)
        command = json['Message']

        # 如果下发的消息为applyRecipe ,开始应用配方
        if 'applyRecipe' == command:
            # 获取下发的消息内容
            Command = json['Command']

            # 倒置SourcrID 与DestinationID ,并返回成功信息
            desID = json['SourceID']
            SourceId = json['DestinationID']


            json['DestinationID']=desID
            json['SourceID'] = SourceId
            json['Message']= 'reviced'
            json['Status']=0

            # 将响应消息 由json格式转化为二进制形式
            outPutData = OutPutMessage.getBytes(json)
            # 将二进制响应数据发送回服务器接口
            websocket.send(outPutData)

            logger.debug('replay :' +str(json))

            logger.debug('thread for controll is start !')


            # 分解消息内容
            messageList = Command.split(',')

            print messageList

            privateRecipeId=messageList[3]

            # 调用函数,让所有的控制进程都去下载配方的数据
            downloadCommand(queueList,messageList[0],messageList[1],messageList[3])

            # 新建一个线程 用于初始化区域硬件能力曲线
            initThread = threading.Thread(target=InitRegion, args=(messageList[1], fwdsocket,fwdGateway))
            initThread.start()
            # 区域初始化线程必须完成后,才能往下走
            initThread.join()

            # 新建一个反馈线程
            t = threading.Thread(target=feedback, args=(queueList,messageList,fwdsocket,fwdGateway))
            t.start()

            # 将反馈线程的id添加进线程列表
            threadControl.append(t.ident)


        if 'update' == command:

            taskMessag = ('update','','','')

            desID = json['SourceID']
            SourceId = json['DestinationID']

            json['DestinationID'] = desID
            json['SourceID'] = SourceId
            json['Message'] = 'reviced'
            json['Status'] = 0
            outPutData = OutPutMessage.getBytes(json)
            websocket.send(outPutData)

            for gateway, task, result in queueList:
                task.put(taskMessag)
                if result.get() != 'ready to reboot in 5s':
                    continue
            logger.debug('replay :' + str(json))


        if 'init'==command:
            logger.debug('preparing init the region')
            Command = json['Command']

            initThread = threading.Thread(target=InitRegion, args=(Command,fwdsocket))
            initThread.start()
            initThread.join()

            initThread.start()
            logger.debug('the init Thread begin')

            desID = json['SourceID']
            SourceId = json['DestinationID']

            json['DestinationID'] = desID
            json['SourceID'] = SourceId
            json['Message'] = 'reviced'
            json['Status'] = 0

            outPutData = OutPutMessage.getBytes(json)

            websocket.send(outPutData)

        # 如果收到的消息为停止配方,则在此分之下
        if 'stopRecipe'==command:

            Command = json['Command']
            desID = json['SourceID']
            SourceId = json['DestinationID']

            json['DestinationID'] = desID
            json['SourceID'] = SourceId
            json['Message'] = 'reviced'
            json['Status'] = 0

            # 将做好的响应数据转化为二进制数据
            outPutData = OutPutMessage.getBytes(json)
            # 通过socket响应回服务端
            websocket.send(outPutData)

            logger.debug('replay :' + str(json))

            # 检查 线程列表长度, 如果没有数据,运行结束
            if len(threadControl)==0:
                continue

            # 获取线程id
            tId= threadControl[0]

            # 调用关闭线程的方法
            _async_raise(tId, SystemExit)

            logger.debug('thread for controll is stop !')
            
      except Exception,e:
          print e.message
          logger.error(e)
          task, result = getTaskAndResult()
          queueList = []
          temp = ('', task, result)
          queueList.append(temp)

          ld = getTheGatewayId()
          nodeList = []
          nodeList.append((ld, ''))

# 该方法是python脚本的启动main函数
if __name__ == '__main__':

    # 获取通信队列
    task, result = getTaskAndResult()
    templist=[]
    temp=('',task, result)

    # 写入队列
    templist.append(temp)

    # 获取网关id
    ld = getTheGatewayId()
    nodelist=[]
    # 写入网关管理数据列表
    nodelist.append((ld,''))

    # 启动main方法
    t = threading.Thread(target=main, args=(templist,nodelist,ld))
    t.start()