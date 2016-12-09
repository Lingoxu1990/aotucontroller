#!/usr/bin/env python
# -*- coding: utf-8 -*-
import socket, fcntl, struct

import time,Queue
import UdpValid
import InPutMessage
import OutPutMessage
import ExecutorQueueManager
import ConfigParser
import logging
import platform


def get_local_ip(ifname='eth0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ret = socket.inet_ntoa(inet[20:24])
    return ret




class UdpValidError(StandardError):
    pass

connect_list=[]
# taskQueue=[]
gatwaySocket =0

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

executorSourceId = cf.get('executor','source')
executorPort =cf.get('executor','port')

# 日志定义
logger = logging.getLogger("ExecutorSocketConnect")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("ExecutorSocketConnect.log")

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


def tcplink(sock, addr,gateway):

    logger.debug('Accept new connection from %s:%s...' % addr)

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

def getSocket(gateway):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    osPlatporm = platform.system()

    if osPlatporm == 'Linux':
        localip = get_local_ip()
        s.bind((localip, int(executorPort)))
        logger.debug('Fwd tcp is ready,and the local IP is %s and the max connection is %d' % (localip, 20))
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        s.bind((localhost[0], int(executorPort)))
        logger.debug('Fwd tcp is ready,and the local IP is %s and the max connection is %d' % (localhost[0], 20))

    s.listen(20)

    print("tcp is ready")
    udpValid = {}
    udpValid['Command'] = ""
    udpValid['DestinationID'] = "0000000000000001"
    udpValid['List'] = []
    udpValid['Message'] = "SLAVE"
    udpValid['PackageNumber'] = executorPort
    udpValid['SourceID'] = gateway
    udpValid['Status'] = 0
    udpValid['Type'] = "NULL"

    logger.debug('trying to catch the fwd gateway.....')

    flag = UdpValid.udpValid(udpValid,logger)

    if flag == False:
        logger.error('Can not find the fwd-gateway ')
        raise UdpValidError('Can not find the fwd-gateway ')


    logger.debug('web tcp is Waiting for connection from fwd-gateway')

    sock, addr = s.accept()
    logger.debug('Accept new connection from %s:%s...' % addr)

    sock1 = tcplink(sock, addr,gateway)
    return sock1