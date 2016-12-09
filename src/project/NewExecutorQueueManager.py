#!/usr/bin/env python
import time, sys, Queue
from multiprocessing.managers import BaseManager
class QueueManager(BaseManager):
    pass
def getTaskAndResult():
    QueueManager.register('G000000010052c779_task_queue')
    QueueManager.register('G000000010052c779_result_queue')
    server_addr = '192.168.1.4'
    print('Connect to server %s...' % server_addr)
    m = QueueManager(address=(server_addr, 5000), authkey='abc')
    m.connect()
    task = m.G000000010052c779_task_queue()
    result = m.G000000010052c779_result_queue()
    return task,result
