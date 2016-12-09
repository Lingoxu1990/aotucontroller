#!/usr/bin/env python
import random, time, Queue
from multiprocessing.managers import BaseManager
class QueueManager(BaseManager):
    pass
task_queue_000000010052c779 = Queue.Queue()
result_queue_000000010052c779 = Queue.Queue()
QueueManager.register('G000000010052c779_task_queue', callable=lambda: task_queue_000000010052c779)
QueueManager.register('G000000010052c779_result_queue', callable=lambda: result_queue_000000010052c779)
server_addr = '192.168.1.4'
manager = QueueManager(address=(server_addr, 5000), authkey='abc')
manager.start()
def getTaskAndResult():
    list=[]
    list.append(('000000010052c779',manager.G000000010052c779_task_queue(), manager.G000000010052c779_result_queue()))
    print 'dispatcher get the task and result'
    return list
def shotdownManager():
    manager.shutdown()