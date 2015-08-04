#! /usr/bin/python
#-*- coding: utf-8 -*-


from gevent.queue import Queue
from gevent.lock import BoundedSemaphore

# named.conf中数据存放的list
global view_name_list
view_name_list = []
# view name 的锁
global view_name_sem
view_name_sem = BoundedSemaphore(1)

# 读取文件开始时的时间
global start_time
start_time = [0,0]

# 终止程序标志位,上传程序停止标志位
global bStopPro
bStopPro = [False,False]

# 写入带宽文件的文件描述符
global flux_fd
flux_fd = [0,0]
# 写入带宽文件的文件描述符的锁
global flux_fd_sem
flux_fd_sem = BoundedSemaphore(1)

# 写入流量文件log的文件描述符
global flow_fd
flow_fd = [0,0]
# 写入流量文件log的文件描述符的锁
global flow_fd_sem
flow_fd_sem = BoundedSemaphore(1)

# 服务1的读取文件的队列
global queue_task_date1
queue_task_date1 = Queue(5)
# 服务2的读取文件的队列
global queue_task_date2
queue_task_date2 = Queue(5)


