#!/usr/bin/env python
# -*- coding: utf-8 -*-

import socket
import ConfigParser
import logging
import SqliteUtil
import threading
import json
import fcntl
import struct
import traceback
import os
import platform
import time
import OutPutMessage
import InPutMessage
import random, time, Queue
from multiprocessing.managers import BaseManager
import Dispatcher
import Executor
import inspect
import ctypes
from logging.handlers import TimedRotatingFileHandler


cf = ConfigParser.ConfigParser()
osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

clientGroup = cf.get('clusterClient','group')
clientPort =cf.get('clusterClient','port')

basisPath=cf.get('dbPath', 'basis')

#日志打印格式
log_fmt = '%(asctime)s\tFile \"%(filename)s\",line %(lineno)s\t%(levelname)s: %(message)s'
formatter = logging.Formatter(log_fmt)
logging.basicConfig(level=logging.DEBUG)
#创建TimedRotatingFileHandler对象
clusterHandler = TimedRotatingFileHandler(filename="/sbin/Cluster.log", when="D", interval=1, backupCount=2)
clusterHandler.setFormatter(formatter)
logger = logging.getLogger('update')
logger.addHandler(clusterHandler)


# # 日志定义
# logger = logging.getLogger("Cluster")
# # 设置日志输出等级
# logger.setLevel(logging.DEBUG)
# # 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
# fh = logging.FileHandler("Cluster.log")
#
# # 设置文件handler 的日志输出等级
# fh.setLevel(logging.DEBUG)
#
# # 创建流式handler
# ch = logging.StreamHandler()
# # 设置流式hander  的输出等级
# ch.setLevel(logging.ERROR)
#
# # 配置格式化
# formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# # 应用输出格式至 流式输出
# ch.setFormatter(formatter)
# # 应用输出格式至 文件输出
# fh.setFormatter(formatter)
#
# # 将输出器增加至logger(executor)
# logger.addHandler(ch)
# logger.addHandler(fh)



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


def stop_thread(thread):
    _async_raise(thread.ident, SystemExit)


disGatewayId=''
disAddr=''
nodeList=set()
lock = threading.Lock()
threadDispatcher=[]
threadExecutor=[]

def get_local_ip(ifname='eth0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ret = socket.inet_ntoa(inet[20:24])
    return ret

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

def setTheLeader(param,param2):
    global disGatewayId, lock,disAddr

    lock.acquire()
    disGatewayId=param
    disAddr=param2
    lock.release()

def getTheLeader():
    global disGatewayId, lock,disAddr

    lock.acquire()
    temp = disGatewayId
    temp2 = disAddr
    lock.release()
    return temp,temp2

def reviceMessgae(arg1,sock):

    while True:

        buffer,addr = sock.recv_from(2048)
        jsonObject = InPutMessage.getJson(buffer)
        logger.debug('recived  the response form other dispather: %s'%(str(jsonObject)))


        if jsonObject['code']=='-1':
            tempGateway = jsonObject['content']
            leader = getTheLeader()
            if tempGateway<leader:
                setTheLeader(tempGateway,addr)

def sendMessage(arg1,sock):

  while True:
    boss = {}
    boss['code'] = '0'
    boss['message'] = 'leader'
    boss['content'] =  getTheGatewayId()

    outPutData = OutPutMessage.getBytes(boss)

    sock.sendto(outPutData, (clientGroup, int(clientPort)))
    time.sleep(1)


def udpClient():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    logger.debug('the cluster group is %s and the port is %s ' % (clientGroup, clientPort))

    osPlatporm = platform.system()

    localIp = ''
    if osPlatporm == 'Linux':
        localIp=get_local_ip()
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        localIp=localhost[0]

    setTheLeader(getTheGatewayId(),localIp)

    sender = threading.Thread(target=sendMessage, args=('', sock))
    sender.start()
    # revicer = threading.Thread(target=reviceMessgae, args=('',sock))
    # revicer.start()

def udpServer():

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.bind(('',int(clientPort)))
    logger.debug('udp server is ready,and the port is %s' % (clientPort))


    while 1:
        try:
            message, address = s.recvfrom(2048)

            message=InPutMessage.getJson(message)


            tempGateway = message['content']
            nodeList.add((tempGateway, address))

            tempLeader = getTheLeader()
            if message['content']<tempLeader:
                setTheLeader(message['content'],address)
                message['code']='0'
                message['message']='you are the leader'
                message['content']=''
                outPutData = OutPutMessage.getBytes(message)
                s.sendto(outPutData, address)
            else:
                message['code']='-1'
                message['message']='I am the leader'
                message['content']=tempLeader

                outPutData = OutPutMessage.getBytes(message)
                s.sendto(outPutData, address)
                # logger.debug('the leader is %s and the addresss is %s' % (getTheLeader()))

        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            traceback.print_exc()

def dispathcerRun(queueList, nodeList, leaderGatewayId):
    Dispatcher.main(queueList, nodeList, leaderGatewayId)
def executorRun(task, result, localGatewayId):
    Executor.main(task, result, localGatewayId)


def dispatcherRegister():
    global  threadDispatcher

    print 'prepare the thread dispatcher.......'

    while True:
      time.sleep(5)
      localGatewayId=getTheGatewayId()
      leaderGatewayId,addr = getTheLeader()

      if localGatewayId == leaderGatewayId:
          print 'local host is the leader'
          if len(threadDispatcher) == 0:
              print 'there is no dispatcher thread , starting the dispathcer.....'
              dispatcherQueueManagerCodeProducer()

              queueList=None
              try:
                  import NewDispatcherQueueManager
                  print 'QueueManager code for the dispathcer is ready'
                  queueList = NewDispatcherQueueManager.getTaskAndResult()
                  print 'QueueManager code for the dispathcer is gernerated'
              except Exception, e:
                  print 'QueueManager code is error'
                  print e.message

              print 'startint the dispathcer thread'
              dispathcer = threading.Thread(target=dispathcerRun, args=(queueList, nodeList, leaderGatewayId))
              dispathcer.setDaemon(True)
              dispathcer.start()
              print 'save the dispather into a queue'
              threadDispatcher.append((dispathcer.ident,dispathcer))
      else:
          print 'local host is not the leader, check the dispathcer'
          if len(threadDispatcher) != 0:
              print 'there is a dispathcer in the queue , killing the dispatcher'
              stop_thread(threadDispatcher[0][1])
              threadDispatcher=[]


def executorRegister():
    localGatewayId = getTheGatewayId()
    leaderGatewayId, tempAddr = getTheLeader()

    global threadExecutor

    while True:
        time.sleep(5)
        localGatewayId = getTheGatewayId()
        leaderGatewayId, addr = getTheLeader()

        if addr!=tempAddr:
            logger.debug('the leader has been changed,kill the old executor, and create a new executor')

            if len(threadExecutor) != 0:
                stop_thread(threadExecutor[0][1])
                threadExecutor = []
            if len(threadExecutor) == 0:
                executorQueueManagerCodeProducer()
                task=None
                result=None
                try:
                    import NewExecutorQueueManager
                    task, result = NewExecutorQueueManager.getTaskAndResult()
                except Exception, e:
                    print e

                executor = threading.Thread(target=executorRun, args=(task, result, localGatewayId))
                executor.setDaemon(True)
                executor.start()
                threadExecutor.append((executor.ident, executor))
        else:


            logger.debug('the leader is the original one ')

            if len(threadExecutor) == 0:
                logger.debug('there is no executor , create a executor')

                executorQueueManagerCodeProducer()
                task = None
                result = None
                try:
                    import NewExecutorQueueManager
                    task, result = NewExecutorQueueManager.getTaskAndResult()
                except Exception, e:

                    print e.message

                executor = threading.Thread(target=executorRun, args=(task, result, localGatewayId))
                executor.setDaemon(True)
                executor.start()
                threadExecutor.append((executor.ident, executor))


def dispatcherQueueManagerCodeProducer():
    tempFile = open('DispatcherQueueManagerTemp.txt','r+')
    totableLines = tempFile.readlines()
    tempFile.close()

    part1Lines=totableLines[:5]

    queueLines=totableLines[5:9]
    portLines=totableLines[9:10]

    part3Lines=totableLines[10:14]

    apiLines=totableLines[14:15]

    part5Lines=totableLines[15:]

    queueLinesCodes=[]
    apiLinesCodes=[]
    for gateway, addr in nodeList:
        temp1 = queueLines[:]
        for line in temp1:
            newLine = line.replace('$',gateway)
            queueLinesCodes.append(newLine)

        temp2=apiLines[:]
        for line in temp2:
            newLine = line.replace('$',gateway)
            apiLinesCodes.append(newLine)

    osPlatporm = platform.system()

    localip=''
    if osPlatporm == 'Linux':
        localip = get_local_ip()
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        localip=localhost[0]

    newPortLine = []
    for line in portLines:
        newPortLine.append(line.replace('&',localip))


    goalFile = open('NewDispatcherQueueManager.py','w+')

    for line in part1Lines:
        goalFile.write(line)
    for line in queueLinesCodes:
        goalFile.write(line)
    for line in newPortLine:
        goalFile.write(line)
    for line in part3Lines:
        goalFile.write(line)
    for line in apiLinesCodes:
        goalFile.write(line)
    for line in part5Lines:
        goalFile.write(line)
    goalFile.close()

def executorQueueManagerCodeProducer():
    tempFile = open('ExecutorQueueManagerTemp.txt', 'r+')
    totableLines = tempFile.readlines()
    tempFile.close()

    part1Lines = totableLines[:6]

    queueLines = totableLines[6:8]
    print queueLines
    leaderAddr = totableLines[8:9]
    part4Lines = totableLines[9:12]
    apiLines = totableLines[12:14]
    part6Lines = totableLines[14:]

    localGateway = getTheGatewayId()
    leaderGateway,addr = getTheLeader()
    # localGateway ='G3'
    # addr='192.168.31.102'

    queueCodes=[]
    for line in queueLines:
        newLine = line.replace('$',localGateway)
        queueCodes.append(newLine)
    leaderAddrCode=[]
    for line in leaderAddr:
        newLine = line.replace('&',addr)
        leaderAddrCode.append(newLine)
    apiLinesCode=[]
    for line in apiLines:
        newLine = line.replace('$',localGateway)
        apiLinesCode.append(newLine)


    goalFile = open('NewExecutorQueueManager.py','w+')

    for line in part1Lines:
        goalFile.write(line)
    for line in queueCodes:
        goalFile.write(line)
    for line in leaderAddrCode:
        goalFile.write(line)
    for line in part4Lines:
        goalFile.write(line)
    for line in apiLinesCode:
        goalFile.write(line)
    for line in part6Lines:
        goalFile.write(line)
    goalFile.close()


def main():

    Server=threading.Thread(target=udpServer, args=())
    Server.setDaemon(True)
    Server.start()
    time.sleep(2)
    udpClient()
    dispatcher=threading.Thread(target=dispatcherRegister, args=())
    executor = threading.Thread(target=executorRegister, args=())
    dispatcher.setDaemon(True)
    executor.setDaemon(True)
    dispatcher.start()
    executor.start()


main()


# executorQueueManagerCodeProducer()
