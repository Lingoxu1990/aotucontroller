#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ConfigParser
import httplib
import json
import time


cf = ConfigParser.ConfigParser()
# cf.read("/sbin/config.conf")
# serverAddr = cf.get('uploader','addr')
# serverPort =cf.get('uploader','port')
# serverUri = cf.get('uploader','uri')

def getUtcTime():
    headers = {"Accept": "application/json"}

    httpClient = httplib.HTTPConnection("103.250.227.120", 8089, timeout=3)
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
        return ""



if __name__ == '__main__':
    print getUtcTime()