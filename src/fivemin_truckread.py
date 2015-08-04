#! /usr/bin/python
#-*- coding: utf-8 -*-

import time,gzip,stat,os,string
import hashlib
import fivemin_gl
from fivemin_common import get_logname
from shutil import rmtree
from struct import pack,unpack,calcsize
import gevent.monkey
gevent.monkey.patch_all()

class read_access():
    def __init__(self,config,log):
        # errorline file descriptor
        self.errorlinefd = [0,0]
        # 配置文件 
        self.config = config
        # log记录 
        self.log = log
        # url推送文件字典信息 
        self.file_list = []
        # url推送哈希表信息
        self.url_list = []
        # fd打开的数目
        self.cur_fd = [0,0]
        # 打开access.log文件操作次数
        self.iNumber_ori = 0

    # 开启读取服务初始化 
    def ready_to_read(self):
        for i in range(self.config.FILE_NAME):
            tmpfile_dir = {}
            tmpurl_dir = {}
            # errorline的文件名字 
            szlogPath = "%s/%ld.%derrline" %(self.config.SERVICE_LIST[i]["OUTPATH"],time.time(),i)
            # 创建错误的access.log存放的文件描述符 
            self.errorlinefd[i] = open(szlogPath, "aw", 00600)
            self.file_list.append(tmpfile_dir)
            self.url_list.append(tmpurl_dir)
            try:
                # 清空推送文件夹url
                self.ready_del_url(i)
                self.log.debug( "Fivemin_truckread-ready_to_read-error file created succeed:%d",i)
            except IOError as ex:
                self.log.error( "Fivemin_truckread-ready_to_read-error file created succeed:%s",str(ex))
                return False
        return True
    
    # 关闭错误文件 
    def end_to_read(self):
        for i in range(self.config.FILE_NAME):
            self.errorlinefd[i].close() 
            # 删除文件描述符
            self.close_fd(i,True)           
    
    # 定时更新URL缓存
    def delete_url(self,iNumber):
        # 如果没有开启收集url的开关
        if not self.config.SERVICE_LIST[iNumber]["EXTRAURL"]:
            return 0
        self.log.debug("======start to swap======")
        # squid缓存结构体长度
        isquid_len = calcsize("ci4dq2h16s")
        # cache文件
        for cache_path in self.config.SQUID_SER_CACHE[iNumber]:
            with open(cache_path,"rb") as cache_fd:
                # 读取squid缓存数据
                date = cache_fd.read(isquid_len)
                if not date:
                    break
                # 解压squid的缓存数据
                squid_op,squid_swapfilen,squid_timestamp,squid_laastref,squid_expires,\
                squid_lastmod,squid_swapfilesize,squid_refcount,squid_flags,squid_key = unpack("@ci4dq2h16s",date)
                # 如果在缓存中则设置标志位
                if squid_key in self.url_list[iNumber].keys():
                    self.url_list[iNumber][squid_key] = True
        self.log.debug("start to check service[%d] url:%d",iNumber,len(self.url_list[iNumber]))
        delete_file_list = []
        # url文件轮询
        for file_key,file_value in self.file_list[iNumber].iteritems():
            # url缓存更新文件夹的绝对路劲
            file_path = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["URLPATH"],file_key)
            # 临时文件的绝对路劲
            file_tmppath = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["URLPATH"],"tmpdomain")
            # squid缓存更新存在的数据
            szWrite = ""
            # 文件夹的总大小
            iWritelen = 0
            # 临时文件的文件描述符
            iTmp_fd = -1
            #self.log.debug("progress url:%s",file_key)
            # 打开文件，开始处理
            with open(file_path,"r+") as file_fd:
                for date in file_fd:
                    # 解析数据
                    date_list = date.split("^")
                    # 如果数据不合法
                    if len(date_list) > 2:
                        continue
                    url,url_state = date_list
                    url_key = hashlib.new("md5",url).digest()
                    # 数据不在内存中url
                    if url_key not in self.url_list[iNumber].keys():
                        # 不保存数据，处理下一行
                        continue
                    # 数据不再squid缓存中
                    if not self.url_list[iNumber][url_key]:
                        # 删除内存中url的数据 
                        del self.url_list[iNumber][url_key]
                    # 数据在squid缓存中
                    else:
                        # 记录数据
                        szWrite = szWrite+date 
                        # 重新设置缓存标志位
                        self.url_list[iNumber][url_key] = False
                        # 文件较大的读取临时文件
                        if len(szWrite) > 40*1024:
                            # 临时文件描述符是否开启
                            if iTmp_fd == -1:
                                iTmp_fd = open(file_tmppath,"w")
                            # 写入文件
                            iTmp_fd.write(szWrite)
                            szWrite = ""
                # 临时文件的处理
                if iTmp_fd != -1:
                    # 有剩余内容，写入文件
                    if szWrite:
                        iTmp_fd.write(szWrite)
                    # 关闭文件
                    iTmp_fd.close()
                # 文件内容不大处理
                else:
                    # 有文件内容写入
                    if szWrite:
                        # 写入文件
                        iWritelen = len(szWrite)
                        file_fd.write(szWrite)
                        file_fd.truncate(iWritelen)
            # 临时文件的话重命名
            if iTmp_fd != -1:
                os.remove(file_path)
                os.rename(file_tmppath,file_path)
            # 本文件如果没有内容侧删除文件
            else:
                if iWritelen == 0:
                    # 关闭打开着的文件描述符
                    if file_value[0] != -1:
                        os.close(file_value[0])
                        self.cur_fd[iNumber] -= 1
                    # 清除文件字典中file_key
                    delete_file_list.append(file_key)
                    # 删除文件
                    os.remove(file_path)
        # 删除file文件的key值
        for key_value in delete_file_list:
            del self.file_list[iNumber][key_value]
        # 清空list
        delete_file_list = []
        self.log.debug("end to check service[%d] url:%d",iNumber,len(self.url_list[iNumber]))
        self.log.debug("end to check service[%d] file:%d",iNumber,len(self.file_list[iNumber]))
        return 0

    # 初始化删除之前记录的url
    def ready_del_url(self,iNumber):
        if not self.config.SERVICE_LIST[iNumber]["EXTRAURL"]:
            return 0
        szPath =  self.config.SERVICE_LIST[iNumber]["URLPATH"]
        # 删除目录以及目录以下内容
        rmtree(szPath)
        # 重新创建目录
        os.mkdir(szPath)
        return 0

    # 读取access.log的主循环 
    def read_to_queue(self):
        # 创建错误log文件描述符 
        self.ready_to_read()   
        # 循环log时间 
        tTime_Loop = time.time()
        # 循环URL更新时间
        tUrl_day = 0
        # 处理access.log
        self.progresslog()
        while 1:
            # access.log刷新时间
            tTme_Now = time.time()
            # 更新Url缓存时间
            tUrl_Now = time.localtime()
            # 1分钟处理，防止频繁处理log 
            if (tTme_Now - tTime_Loop) > 60:
                tTime_Loop = tTme_Now
                self.progresslog()
            # 定时更新URL缓存
            if ((tUrl_day != tUrl_Now.tm_mday) and (tUrl_Now.tm_hour == 13) and ((tUrl_Now.tm_min - 10) < 8)):
                for i in range(self.config.FILE_NAME):
                    # 开始更新Url
                    self.delete_url(i)
                # 刷新更新Url时间
                tUrl_day = tUrl_Now.tm_mday
            # 退出5程序
            if fivemin_gl.bStopPro[0]:
                self.log.debug("Fivemin_truckread-read_to_queue-program will exit") 
                # 等待上传程序，处理log程序结束
                if self.wait_stop():
                    self.log.debug("Fivemin_truckread-read_to_queue-read exit")
                    break
            gevent.sleep(5)
        # 关闭错误的access.log文件
        self.end_to_read
        return

    # 等待退出
    def wait_stop(self):
        gevent.sleep(40)
        # 上传进程关闭成功
        if fivemin_gl.bStopPro[1]:
            # 轮询Service
            for i in range(self.config.FILE_NAME):
                self.log.debug("Fivemin_truckread-wait_stop-truck exit")
                # 如果开启流量文件，则关闭
                if fivemin_gl.flow_fd[i]:
                    fivemin_gl.flow_fd[i].close()
                    # 将流量临时文件重命名
                    szflow_path_name = get_flowlogname(self.config.BING_IP[i])
                    tmpflow_name = "%s/flow.tmp%d.log" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],i)
                    flow_name = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],szflow_path_name)
                    os.rename(tmpflow_name,flow_name)
                    fivemin_gl.flow_fd[i] = 0
                # 如果开启带宽文件，则关闭
                if fivemin_gl.flux_fd[i]:
                    fivemin_gl.flux_fd[i].close()
                    # 将带宽临时文件重命名
                    szflux_path_name = get_fluxname(self.config.BING_IP[i])
                    tmpflux_name = "%s/flux.tmp%d.gz" %(self.config.SERVICE_LIST[iNumber]["FLUXPATH"],i)
                    flux_name = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLUXPATH"],szflux_path_name)
                    os.rename(tmpflux_name,flux_name)
                    fivemin_gl.flux_fd[i] = 0
            return True
        # 上传进程没有结束，则继续等待
        else:
            self.wait_stop()

    # 轮询读取access.log
    def progresslog(self):
        for i in range(self.config.FILE_NAME):
            self.log.debug("Fivemin_truckread-progresslog-Service %d analysis log ",i)
            # 开始读取文件
            if self.readfile(i) < 0:
                self.log.error("Fivemin_truckread-progresslog-Service %d analysis log error",i)
        return 0
    
    # 检查文件信息
    def check_filelog(self,iNumber,path):
        # 检查文件是否存在
        if not os.path.exists(path):
            return -2
        # 获取文件状态
        file_stat = os.stat(path)
        if None != file_stat:
            # 文件夹内容的size
            if file_stat [ stat.ST_SIZE ] > 1024 * 1024 * 512:
                self.log.error("Fivemin_truckread-check_filelog-%s:larger than 512M",path)
            self.log.debug("Fivemin_truckread-check_filelog- %s size:%ld K",path,(file_stat [ stat.ST_SIZE ]/1024))
        else:
            self.log.error("Fivemin_truckread-check_filelog-stat %s error",path)
            return -1
        return 0

    # 对一行数据进行处理 
    def do_squid(self,szline,date_list,iNumber):
        # 分割一行数据 
        line_list = szline.split('\t')
        # squid日志有37个字段
        #if len(line_list) != 37 and len(line_list) != 19:
        if len(line_list) < 19:
            # 无法处理的access.log日志放入errorline
            self.log.error("Fivemin_truckread-do_squid-Service[%d] error read access.log line",iNumber) 
            if self.errorlinefd[iNumber] > 0:
                self.errorlinefd[iNumber].write(szline)
                return -1
        # 初始化5分钟切割文件的时间戳 
        if fivemin_gl.start_time[iNumber] == 0:
            fivemin_gl.start_time[iNumber] = int(string.atof(line_list[1]))    
        # timestamp,requestUrl,clientip,chahe_code,cache_size,last_time,hier_code
        lineinfo = [line_list[1],line_list[8],line_list[0],line_list[5],line_list[6],line_list[17],line_list[12]]
        # 加入list
        date_list.append(lineinfo)
        if not self.config.SERVICE_LIST[iNumber]["EXTRAURL"]:
            self.collect_Url(line_list[8],line_list[5],iNumber)
        return 0 
    
    # Url推送相关
    def collect_Url(self,url,url_status,iNumber):
        # 切割域名
        url_list = url.split("/")
        # 排除无效的Url
        if len(url_list) < 3:
            return -1
        else:
            domain = url_list[2]
        # 排除不规则的Url和域名
        if 255 < len(url) or 1 > len(domain):
            return -1
        # 域名转化为md5值
        hash_key = hashlib.new("md5",url).digest()
        # 重复的Url
        if hash_key in self.url_list[iNumber].keys():
            return 0
        # 拼接写入内容
        szWri = "%s^%s\n" %(url,url_status)
        #szWri = pack("256s32s",url,url_status)
        # 文件夹fd的时间戳
        file_time = int(time.time())
        # 拼接文件域名的绝对路径
        szdomain_file = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["URLPATH"],domain)
        # 判断打开的文件描述符已经达到最大值
        self.close_fd(iNumber,False)
        # 文件已经开启
        if domain in self.file_list[iNumber].keys():
            # 开启的文件描述符已经被关闭
            if self.file_list[iNumber][domain][0] == -1:
                file_fd = os.open(szdomain_file,os.O_RDWR|os.O_CREAT)
                self.cur_fd[iNumber] += 1
                # 修改域名文件字典
                self.file_list[iNumber][domain][0] = file_fd
            # 写入文件
            os.write(self.file_list[iNumber][domain][0],szWri)
            # 添加url字典的value
            self.url_list[iNumber][hash_key] = False
        # 域名文件没有
        else:
            # 创建文件 
            file_fd = os.open(szdomain_file,os.O_RDWR|os.O_CREAT)
            self.cur_fd[iNumber] += 1
            try :
                # 写入文件
                os.write(file_fd,szWri)
                # 域名字典中内容：文件描述符，时间戳，url位置
                file_value = [file_fd]
                # 添加域名文件字典
                self.file_list[iNumber][domain] = file_value
                # 添加url字典的value
                self.url_list[iNumber][hash_key] = False
            except IOError as ex:
                print "error"
                return -1
        return 0
    
    # 关闭早的文件描述符
    def close_fd(self,iNumber,exit_flag):
        # 超过上限就关闭文件描述符
        if (self.cur_fd[iNumber] < 9999) and not exit_flag:
            return 0
        for key,valus in self.file_list[iNumber].iteritems():
            # 关闭所有的描述符
            if valus[0] != -1:
                os.close(valus[0])
                valus[0] = -1
                self.cur_fd[iNumber] -= 1
        return 0

    # 加入队列
    def add_queue(self,date_list,iNumber):
        # 有数据就加入队列    
        if date_list:
            # 加入service1的队列处理 
            if 0 == iNumber:
                # 队列已满，等待5s时间处理
                if fivemin_gl.queue_task_date1.full():
                    self.log.debug("Fivemin_truckread-readfile-Service%d:queue_task_date1 is full",iNumber) 
                    gevent.sleep(5)
                fivemin_gl.queue_task_date1.put(date_list,block = True, timeout = 20)
            # 加入service2的队列处理 
            elif 1 == iNumber:
                # 队列已满，等待5s时间处理
                if fivemin_gl.queue_task_date2.full():
                    self.log.debug("Fivemin_truckread-readfile-Service%d:queue_task_date1 is full",iNumber) 
                    gevent.sleep(5)
                fivemin_gl.queue_task_date2.put(date_list,block = True,timeout = 20)
        

    # 读取文件
    def readfile(self,iNumber):
        # 检查access.log文件的信息
        szAccessPath = self.config.SERVICE_LIST[iNumber]["INPATH"]
        if self.check_filelog(iNumber,szAccessPath) < 0:
            return -1
        # 检查log.writing的文件信息
        szWritepath = "%s/log.writing%d" %(self.config.SERVICE_LIST[iNumber]["OUTPATH"],iNumber)
        if -1 == self.check_filelog(iNumber,szWritepath):
            return -1
        # access.log文件描述符
        Accfd = open(szAccessPath, 'r+')
        # 读取后的文件的描述符 
        Wrifd = gzip.open(szWritepath, "ab+")
        # 写入原始日志文件的buff
        szWriBuf = ""
        # 读取文件行数，5000行为一个队列 
        ilineNumber = 0
        # 是否继续读取文件的flag
        bcontstatus = False
        # 读取行数，防止读取文件过快，处理程序来不及处理 
        ilinetimes = 0
        # 数据太少，原始日志一定次数过后自动打包
        self.iNumber_ori += 1
        try:
            while 1:
                # 队列list 
                date_list = []
                # 读取5000行后，文件没有结束，继续读取 
                bcontstatus = False
                for szline in Accfd:              
                    # 读取文件速度控制 
                    ilinetimes += 1
                    if 512 < ilinetimes:
                        ilinetimes = 0
                        gevent.sleep(0.001)
                    # 不同的平台录入不同的数据
                    if 0 == self.config.SERVICE_LIST[iNumber]["DATAFORMAT"]:
                        if self.do_squid(szline,date_list,iNumber) < 0:
                            continue
                    if 1 == self.config.SERVICE_LIST[iNumber]["DATAFORMAT"]:
                        if self.do_squid(szline,date_list,iNumber) < 0:
                            continue
                    
                    # 写入原始日志
                    szWriBuf =  szWriBuf+szline
                    if len(szWriBuf) > 1024*40 :
                        Wrifd.write(szWriBuf)
                        szWriBuf = ""
                    # 读取的有效行数计数
                    ilineNumber += 1
                    #读取5000行数据
                    if ilineNumber > 4999:
                        ilineNumber = 0
                        bcontstatus = True
                        break
                # 数据加入队列
                self.add_queue(date_list,iNumber)
                # 如果没有读完，继续读取access.log
                if bcontstatus:
                    continue
                # 读取文件完毕，跳出循环
                break
            # 原始数据写入
            if len(szWriBuf) > 0:
                Wrifd.write(szWriBuf)
                szWriBuf = ""
            self.log.debug("Fivemin_truckread-readfile-Cut file OK")
            # 清空access.log内容
            Accfd.truncate(0)
            # 关闭文件
            Accfd.close()
            Wrifd.close()  
            # 原始日志文件处理 
            self.creat_rawlog(szWritepath,iNumber)
            return 0
        except Exception as ex:
            self.log.error("Fivemin_truckread-readfile-%s",str(ex))
            # 关闭相关文件描述符
            Accfd.close()
            Wrifd.close() 
            return -1  
    
    # 把原始日志重名并且转移至原始日志文件夹
    def creat_rawlog(self,oldpath,iNumber):
        #原始日志超过40M 就重新开文件
        szName = get_logname(self.config.BING_IP[iNumber])
        szPathName = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["LOGPATH"],szName)
        Wrifile_stat = os.stat(oldpath)
        # 文件属性 
        if None != Wrifile_stat:
            ifile_size = Wrifile_stat[stat.ST_SIZE]
            if ifile_size > 0:
                if (ifile_size > 1024*1024*40) or (self.iNumber_ori > 60*6):
                    # 重命名文件，把文件转移到原始日志文件夹
                    os.rename(oldpath, szPathName)
                    self.log.debug("Fivemin_truckread-readfile-rename file ok writing->access.gz")
        else:
            self.log.error("Fivemin_truckread-readfile-stat log.writing file error")
            # 重命名文件，把文件转移到原始日志文件夹
            os.rename(oldpath, szPathName)   

         
