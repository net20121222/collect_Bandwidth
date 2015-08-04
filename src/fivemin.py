#! /usr/bin/python
#-*- coding: utf-8 -*-
import sys,string,os,signal,gevent,time,signal
import subprocess
import gevent.monkey
from fivemin_gl import view_name_list,bStopPro
from fivemin_structed import ViewName
from fivemin_config import Config 
from fivemin_truckread import read_access
from fivemin_truck import progress_access
from fivemin_log import logger
from fivemin_upfile import client
#from multiprocessing import Process
gevent.monkey.patch_all() 

if __name__ == "__main__":
    # 判断程序是否运行
    def progress_run_status(fivemin_log):
        # 是否运行的flag 
        bflag = False
        # 运用shell命令 
        szCommond = 'ps aux|grep fivemin.py|grep -v "grep"'
        # 获取当前进程的pid 
        iPid = os.getpid()
        # 执行命令 
        date = subprocess.Popen(szCommond,stdout = subprocess.PIPE,stderr = subprocess.PIPE,shell = True)
        # 等待结果 
        date.wait()
        # 获取数据内容 
        szStatus = date.stdout.readlines()
        # 处理数据 
        for i in xrange(len(szStatus)):
            # 去除当前进程的情况 
            if iPid == int(szStatus[i].split()[1]):
                continue
            # 查找启动文件 
            if -1 != szStatus[i].find("fivemin.py"):
                # 命令输出数据信息太少的情况 
                if (len(szStatus[i]) >= 12):
                    fivemin_log.error("Progress ID:%s    Progress Name:%s",szStatus[i].split()[1],szStatus[i].split()[11])
                else:
                    fivemin_log.error("%s",szStatus[i])
                bflag = True
        if bflag:
            return True
        else:
            return False
    
    # 把string类型的ip转换成数字
    def ip_into_int(ip):
        return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))    

    # 录入view named数据
    def load_view_data(szpath,fivemin_log):
        if not os.path.exists(szpath):
            fivemin_log.error("name.conf is not exist")
            return -1	
        try:
            with open(szpath) as ViewNamefd:
                for line in ViewNamefd:
                    # 获取数据类似220.11.11.11/26i;220.22.22.22/26 
                    net_ip = line.split()[4].replace("{","").replace("}","").rstrip(";").split(";")
                    # 处理220.11.11/26,list包含32个class
                    for net_ip_line in net_ip:
                        # 生成[220.11.11,26] 
                        network = net_ip_line.split('/') 
                        mask = 0xffffffff
                        mask <<= 32-int(network[1])
                        # ip转化为int类型 
                        realmask = ip_into_int(network[0])
                        # 判断是否存在mask的flag 
                        bViewflag = True                
                        # 轮询list 
                        for iNum in range(len(view_name_list)):
                            if(mask == view_name_list[iNum].mask_t):
                                view_name_list[iNum].netid_t[realmask] = line.split()[1]
                                bViewflag = False
                                break
                        # 如果不在list中间，则添加到list 
                        if(bViewflag):
                            # 新建结构体
                            View_ip_Infor = ViewName()
                            View_ip_Infor.mask_t = mask
                            View_ip_Infor.netid_t[realmask] = line.split()[1]
                            view_name_list.append(View_ip_Infor)
        except IOError as ex:
            fivemin_log.error("Read named.conf Error:%s",str(ex))
            return -1
        return 0

    # 读取配置文件
    def load_config(fivemin_log):
        # 生成配置文件对象 
        fivemin_config = Config(fivemin_log)
        # 读取配置文件
        if fivemin_config.read_config() < 0:
            fivemin_log.error("Read Config failed")
        return fivemin_config

    # 读取log配置，生成log对象
    def load_log():
        # log的文件名
        log_name= "%s_%s" %(time.strftime("%Y-%m-%d"),"fivemin.log")
        szCur_tmp = os.getcwd()
        szCur_path = os.path.dirname(szCur_tmp)
        # 5分钟存放日志的文件夹的绝对路径
        szlog_Path = "%s/%s" %(szCur_path,"log")
        if not os.path.exists(szlog_Path):
            os.mkdir(szlog_Path)       
        # log文件的绝对路径 
        szlogfile_path="%s/%s" %(szlog_Path,log_name)
        #创建日志对象
        log=logger(szlogfile_path)
        #返回日志对象
        return log   

    # 单独开始处理日志文件
    def start_truck():
        # 启动功能list
        thread = []
        # load config   
        five_config = load_config()
        # load log config
        truckName = "%s-%s" %(time.strftime("%Y%m%d"),"truck.log")
        fivemin_log = load_log(truckName)
        # load IPname area
        if load_view_data(five_config.IPNAMEDPATH) < 0:
            print "load_view_data failed"
            return -1
        # 创建功能对象 
        read_accesslog = read_access(five_config,fivemin_log)
        work1 = gevent.spawn(read_accesslog.read_to_queue)
        thread.append(work1)
        for i in range(10):
            worker = progress_access(five_config, fivemin_log, i)
            work = gevent.spawn(worker.handle_task)
            thread.append(work)
        # 启动功能 
        gevent.joinall(thread)
        print "system start succeed" 

    # 开始上传功能
    def start_upload():
        five_config = load_config()
        sendName = "%s-%s" %(time.strftime("%Y%m%d"),"send.log")
        fivemin_log = load_log(sendName)
        up_file = client(five_config,fivemin_log)
        work = gevent.spawn(up_file.handle_task())
        # swap程序 
        gevent.joinall([work]) 
        print "system start succeed" 
    
    # 初始化ip区域 
    def load_view(five_config,fivemin_log):
        # load IPname area
        if load_view_data(five_config.IPNAMEDPATH,fivemin_log) < 0:
            fivemin_log.error("fivemin-load_view-load view failed")
            sys.exit()
    
    # 初始化分析日志协程
    def start_truckCoroutine(five_config,thread,fivemin_log):
        if not five_config.TRUNC_PROCESS_DISABLE:
            # 处理队列协程加入启动队列（10）
            for i in range(10):
                worker = progress_access(five_config, fivemin_log, i)
                work = gevent.spawn(worker.handle_task)
                thread.append(work)
    
    # 上传文件功能初始化
    def start_sendCoroutine(five_config,thread,fivemin_log):
        if not five_config.SEND_PROCESS_DISABLE:
            # 上传对象
            up_file = client(five_config,fivemin_log)
            # 上传协程进入队列
            work = gevent.spawn(up_file.handle_task())
            thread.append(work)
        else:
            bStopPro[1] = True
    
    # 初始化读取access.log功能 
    def start_readCoroutine(five_config,fivemin_log):
        # 处理日志log文件名 
        if not five_config.TRUNC_PROCESS_DISABLE:
            # 创建read对象
            read_accesslog = read_access(five_config,fivemin_log)
            # 开始读取access.log
            read_accesslog.read_to_queue()

    # 中断信号处理
    def signal_work():
        signal.signal(signal.SIGQUIT, sigint_handler)
        signal.signal(signal.SIGINT, sigint_handler)
        signal.signal(signal.SIGHUP, sigint_handler)
        signal.signal(signal.SIGTERM, sigint_handler)
    
    # 信号处理
    def sigint_handler(signum, frame):
        # 设置终止程序
        bStopPro[0] = True

    # 开始启动程序
    def start():
        # log 日志初始化
        fivemin_log = load_log()
        # 判断程序是否运行 
        if True == progress_run_status(fivemin_log):
            fivemin_log.error("fivemin is working")
            return 
        if (len(sys.argv) == 1) :
            # load config   
            five_config = load_config(fivemin_log)
            # load view name表
            load_view(five_config,fivemin_log)
            #中断信号处理
            signal_work()
            # 初始化启动队列
            thread = []    
            # 初始化处理日志队列
            start_truckCoroutine(five_config,thread,fivemin_log)
            # 初始化上传功能队列
            start_sendCoroutine(five_config,thread,fivemin_log)
            # 初始化读取Access.log
            start_readCoroutine(five_config,fivemin_log)
            # 如果单独开启上传功能
            if five_config.SEND_PROCESS_DISABLE and five_config.TRUNC_PROCESS_DISABLE:
                # 开启全部协程
                gevent.joinall(thread)
        # 更具需求启动程序部分功能 
        elif (len(sys.argv) == 2):
            # 只开启处理日志
            if sys.argv[1] == "-truck":
                start_truck()
            # 只开启上传
            elif sys.argv[1] == "-send":
                start_upload()
            # 无效的命令    
            else:
                fivemin_log.error("Wrong Cmd")
        # 错误的命令 
        else:
            fivemin_log.error("Wrong Cmd")
    
    # 开始启动程序        
    start()
    
