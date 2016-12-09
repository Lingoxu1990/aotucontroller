#!/usr/bin/env python
# -*- coding: utf-8 -*-
import random, time, Queue
from multiprocessing.managers import BaseManager
import socket

# 获取一个任务队列 用于发送消息
task_queue = Queue.Queue()
# 获取一个响应队列 用于接收响应
result_queue = Queue.Queue()

# 继承 BaseManager类
class QueueManager(BaseManager):
    pass

# 注册 将任务队列注册至 队列管理器中 ,设置回调函数名称,以及回调队列
QueueManager.register('get_task_queue', callable=lambda: task_queue)
QueueManager.register('get_result_queue', callable=lambda: result_queue)

# 注册 任务队列通信端口5000,以及验证密码abc
manager = QueueManager(address=('', 5000), authkey='abc')

# 启动队列管理器
manager.start()

# 获取任务队列与响应队列
def getTaskAndResult():

    print 'dispatcher get the task and result'
    return manager.get_task_queue() ,manager.get_result_queue()

# 关闭队列管理器
def shotdownManager():
    manager.shutdown()



