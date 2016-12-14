#!/usr/bin/env python
# -*- coding: utf-8 -*-
import BaseHTTPServer
import socket,struct
import fcntl
import platform
import logging
import json
import SqliteUtil
import threading
from io import BytesIO
import os
from wsgiref.simple_server import make_server


import SimpleHTTPServer
import SocketServer

# 日志定义
logger = logging.getLogger("dataSync")
# 设置日志输出等级
logger.setLevel(logging.DEBUG)
# 创建文件handler(handler是用来管理日志的输出的接口 这里使用的是文件输出)
fh = logging.FileHandler("/sbin/dataSync.log")

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

def get_local_ip(ifname='eth0'):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ret = socket.inet_ntoa(inet[20:24])
    return ret


def main():
    # 获取socket类
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # 获取系统平台类型
    osPlatporm = platform.system()

    # 如果是linux系统, 采用get_local_ip() 获取本机ip
    if osPlatporm == 'Linux':
        localip = get_local_ip()
        s.bind((localip, 1214))
        print localip
    else:
        server_name, unknown, localhost = socket.gethostbyname_ex(socket.gethostname())
        print localhost[0]
        s.bind((localhost[0], 1214))

    s.listen(20)
    # statinfo = os.stat('C:\Users\Administrator\Desktop\Basis_Data.db')
    # print statinfo

    while True:
        sock, addr = s.accept()
        data = sock.recv(2048)
        if len(data)<8:
            # 排除传输的数据量小于8字节的情况
            continue
        print data
        # statinfo = os.stat('C:\Users\Administrator\Desktop\pache-tomcat-7.0.73.zip')
        statinfo = os.stat('/root/JenNet_File/Basis_Data.db')
        size =statinfo.st_size
        # with open('C:\Users\Administrator\Desktop\pache-tomcat-7.0.73.zip', 'rb') as f:
        with open('/root/JenNet_File/Basis_Data.db', 'rb') as f:
                buffer = bytearray(size)
                f.readinto(buffer)
                print buffer[100]

                byteio = BytesIO()
                byteio.write(struct.pack('L', 8 + size))
                byteio.write(struct.pack('i', 3))
                byteio.write(buffer)
                sock.send(byteio.getvalue())

if __name__ == '__main__':
    main()

