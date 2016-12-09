#!/usr/bin/env python
# -*- coding: utf-8 -*-
import httplib
import json

def locationInit(params):

    params = json.dumps(params)

    headers = {"Content-type": "application/json"
        , "Accept": "*/*"}

    httpClient=None
    # try:

    httpClient = httplib.HTTPConnection('123.56.230.45', 8088, timeout=3)
    httpClient.request("POST", '/dispatcher_location', params, headers)

    response = httpClient.getresponse()



    if response.status != 200:
            result = False

    strResult = response.read()


    print strResult


    # except Exception :
    #
    #     result = False
    # finally:
    #     if httpClient != None:
    #         httpClient.close()
    #         return result


def test():


    params =  [{'id':'123','account_id':'00000001','dispatcher_gateway':'test','sub_gateway':'testSub'}]
    locationInit(params)


test()