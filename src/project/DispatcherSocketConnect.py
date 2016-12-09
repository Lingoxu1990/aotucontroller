#!/usr/bin/env python
# -*- coding: utf-8 -*-
import socket, fcntl, struct

import threading
import time,Queue
import UdpValid
import InPutMessage
import OutPutMessage
import ExecutorQueueManager
import ConfigParser
import logging
import platform

# udp验证错误 ,继承标准错误
class UdpValidError(StandardError):
    pass

# 获取本机ip (用于linux 操作系统)
def get_local_ip(ifname='eth0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ret = socket.inet_ntoa(inet[20:24])
    return ret

# 获取配置文件读取类
cf = ConfigParser.ConfigParser()

# 获取当前操作系统
osPlatporm = platform.system()

# 根据操作系统,选择不同的配置文件路径
if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")


# 获取调度进程与转发网关之间的通信端口
dispatcherSourceId = cf.get('dispatcher','source')
dispatcherPort =cf.get('dispatcher','port')

# 获取调度进程与转发网关之间的通信端口
dispatcher1SourceId = cf.get('dispatcher1','source')
dispatcher1Port =cf.get('dispatcher1','port')


connect_list=[]
# taskQueue=[]
gatwaySocket =0


# 日志定义
logger = logging.getLogger("DispatcherSocketConnect")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("DispatcherSocketConnect.log")

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

# 验证连接至本地的socket实例(与服务后台通信的)是否来自与转发网关
def webtcplink(sock, addr,gateway):
    logger.debug('trying to valid the connector')

    while True:

        # 循环获取tcp 连接数据
        data = sock.recv(2048)

        bytes = bytearray(data)

        # 将二进制数据转化为 json对象
        json = InPutMessage.getJson(bytes)

        logger.debug('the message received is :'+str(json))
        # 获取消息验证
        message = json['Message']

        # 如果获取的到消息是标准的验证格式
        if '*:jennet' == message:

            json['Message'] = 'login successful'
            json['SourceID'] = gateway
            json['List'] = []
            json['Status'] = 0
            json['Type'] = 'NULL'
            json['Command'] = ''

            # 转换为 二进制格式响应
            outPutData = OutPutMessage.getBytes(json)
            # 发送响应
            sock.send(outPutData)
            logger.debug('the replay is :' + str(json))

            print 'the replay is :' + str(json)

            logger.debug('catch the fwd-gateway successfully  %s:%s'%addr)

            print 'catch the fwd-gateway successfully  %s:%s'%addr

            # 将连接存入队列
            connect_list.append(sock)
            # 如果上述步骤都完成了, 直接返回连接对象。结束循环。
            return sock
        else:
            logger.debug('current connection is not from the fwd-gateway')


# 获取与web后台通信的socket连接
# 1 发送udp 验证包
# 2 如果udp验证包正确发送,转发网关会主动连接已经创建好的socketServer
# 3 验证连接到本地的socket 实例是否来子转发网关
# 4 验证完毕后, 返回socket实例
def getWebSocket(gateway):

    # 获取socket类
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 获取系统平台类型
    osPlatporm = platform.system()

    # 如果是linux系统, 采用get_local_ip() 获取本机ip
    if osPlatporm == 'Linux':
        localip = get_local_ip()
        s.bind((localip, int(dispatcherPort)))
        logger.debug('Web tcp is ready,and the local IP is %s and the max connection is %d' % (localip, 20))
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        s.bind((localhost[0], int(dispatcherPort)))
        logger.debug('Web tcp is ready,and the local IP is %s and the max connection is %d' % (localhost[0], 20))

    s.listen(20)

    print 'Web tcp is ready!'

    # udp验证消息
    udpValid = {}
    udpValid['Command'] = ""
    udpValid['DestinationID'] = "0000000000000001"
    udpValid['List'] = []
    udpValid['Message'] = "SLAVE"
    udpValid['PackageNumber'] = dispatcherPort
    udpValid['SourceID'] = gateway
    udpValid['Status'] = 0
    udpValid['Type'] = "NULL"

    logger.debug('trying to catch the fwd gateway.....')
    print 'trying to catch the fwd gateway.....'


    # 调用udp 验证方法,发送 验证包
    flag = UdpValid.udpValid(udpValid,logger)

    if flag == False:
        raise UdpValidError('Can not find the fwd-gateway ')

    logger.debug('web tcp is Waiting for connection from fwd-gateway')

    print 'web tcp is Waiting for connection from fwd-gateway'

    # 如果验证包是正确的发送到了转发网关,转发网关会主动连接,此处等待转发网关连接
    sock, addr = s.accept()

    logger.debug('Accept new connection from :%s%s...' % addr)

    # 验证本次连接是否来自转发网关
    sock1 = webtcplink(sock, addr,gateway)
    return sock1


# 验证连接至本地的socket实例(仅与转发网关通信)是否来自与转发网关
def fwdtcplink(sock, addr,gateway):
    logger.debug('trying to valid the connector')
    while True:

        data = sock.recv(2048)

        bytes = bytearray(data)

        json = InPutMessage.getJson(bytes)
        logger.debug('the message received is :' + str(json))
        message = json['Message']
        if '*:jennet' == message:
            gatwaySocket = sock
            json['Message'] = 'login successful'
            json['SourceID'] = gateway
            # json['SourceID']=dispatcher1SourceId
            json['List'] = []
            json['Status'] = 0
            json['Type'] = 'NULL'
            json['Command'] = ''
            outPutData = OutPutMessage.getBytes(json)
            sock.send(outPutData)
            logger.debug('the replay is :' + str(json))

            logger.debug('catch the fwd-gateway successfully  %s:%s' % addr)
            connect_list.append(sock)
            return sock
        else:
            logger.debug('current connection is not from the fwd-gateway')

# 获取与转发网关通信的的socket连接
# 1 发送udp 验证包
# 2 如果udp验证包正确发送,转发网关会主动连接已经创建好的socketServer
# 3 验证连接到本地的socket 实例是否来子转发网关
# 4 验证完毕后, 返回socket实例
def getFwdSocket(gateway):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    osPlatporm = platform.system()

    if osPlatporm=='Linux':
        localip = get_local_ip()
        logger.debug('Fwd tcp is ready,and the local IP is %s and the max connection is %d' % (localip, 20))
        s.bind((localip, int(dispatcher1Port)))
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        logger.debug('Fwd tcp is ready,and the local IP is %s and the max connection is %d' % (localhost[0], 20))
        s.bind((localhost[0], int(dispatcher1Port)))

    s.listen(20)

    udpValid = {}
    udpValid['Command'] = ""
    udpValid['DestinationID'] = "0000000000000001"
    udpValid['List'] = []
    udpValid['Message'] = "SLAVE"
    udpValid['PackageNumber'] = dispatcher1Port
    udpValid['SourceID'] = gateway
    udpValid['Status'] = 0
    udpValid['Type'] = "NULL"

    logger.debug('trying to catch the fwd gateway.....')

    flag = UdpValid.udpValid(udpValid,logger)

    if flag == False:
        raise UdpValidError('Can not find the fwd-gateway ')

    logger.debug('fwd tcp is Waiting for connection from fwd-gateway')

    sock, addr = s.accept()

    logger.debug('Accept new connection from %s%s:...' % addr)

    sock1 = fwdtcplink(sock, addr,gateway)
    return sock1

