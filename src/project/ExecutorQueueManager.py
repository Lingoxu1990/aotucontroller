#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time, sys, Queue
from multiprocessing.managers import BaseManager
class QueueManager(BaseManager):
    pass

#获取 本机的任务及相应队列
def getTaskAndResult():
    QueueManager.register('get_task_queue')
    QueueManager.register('get_result_queue')
    server_addr = '127.0.0.1'
    print('Connect to server %s...' % server_addr)
    m = QueueManager(address=(server_addr, 5000), authkey='abc')
    m.connect()
    task = m.get_task_queue()
    result = m.get_result_queue()
    return task,result
