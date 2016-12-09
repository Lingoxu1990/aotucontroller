#!/usr/bin/env python
import socket
import struct
import json
from io import BytesIO

import OutPutMessage
import InPutMessage
import ConfigParser
import platform

cf = ConfigParser.ConfigParser()

osPlatporm = platform.system()

if osPlatporm == 'Linux':
    cf.read("/sbin/config.conf")
else:
    cf.read("./config.conf")

group = cf.get('udpValid','group')
port =cf.get('udpValid','port')



group=group
port=int(port)

def udpValid(udpValid,logger):

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

    outPutBytes = OutPutMessage.getBytes(udpValid)

    logger.debug('the udp group is %s and the port is %d ' % (group, port))

    logger.debug('the valid request is :'+str(udpValid))

    s.sendto(outPutBytes, (group, port))

    buffer = bytearray(1024)
    s.recv_into(buffer)
    recv_json = InPutMessage.getJson(buffer)

    logger.debug('the valid response is :' + str(udpValid))

    valid_Message = recv_json['Message']

    flag = False
    if 'connect' == valid_Message:
        flag = True
        logger.debug('udp valid successfully')
    else:
        logger.debug('failed to valid')

    return flag


