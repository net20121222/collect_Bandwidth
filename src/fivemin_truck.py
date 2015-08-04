#! /usr/bin/python
#-*- coding: utf-8 -*-

import fivemin_gl
from fivemin_common import find_view_area,get_fluxname,get_flowlogname
import gzip,os,string
import gevent.monkey
from json import loads,dumps
gevent.monkey.patch_all()

class progress_access():
    def __init__(self,config,log,threadID):
        # 配置文件内容
        self.config = config
        # log内容
        self.log = log
        # 协程ID
        self.threadID = threadID
    
    # 处理日志的主循环
    def handle_task(self):
        while 1:
            # 轮询处理access.log的内容 
            for i in range(self.config.FILE_NAME):
                # self.log.debug("fivemin_truck-handle_task-progress_task %d now",i)
                self.progress_service(i)
            gevent.sleep(1)
            continue
    
    # 处理日志中Service1和Service2的处理        
    def progress_service(self,iNumber):
        # 服务1的处理 
        if 0 == iNumber:
            while 1:
                # 队列不为空
                if not fivemin_gl.queue_task_date1.empty():
                    # 获取队列信息
                    list_info1 = fivemin_gl.queue_task_date1.get()
                    if self.progress_task(list_info1,0) < 0:
                        self.log.error("fivemin_truck-progress_service-progress_task %d error",iNumber)
                    # 清空队列
                    list_info1= []
                else:
                    break
        # 服务2的处理 
        elif 1 == iNumber:
            while 1:
                # 队列不为空
                if not fivemin_gl.queue_task_date2.empty():
                    # 获取队列信息
                    list_info2 = fivemin_gl.queue_task_date2.get()
                    if self.progress_task(list_info2,1) < 0:
                        self.log.error("fivemin_truck-progress_service-progress_task %d error",iNumber)
                    # 清空队列
                    list_info2 = []
                else:
                    break
        # 未知的错误 
        else:
            self.log.error("fivemin_truck-progress_service-unknow service%d",iNumber) 
            return -1
            
    # 处理access.log的具体过程        
    def progress_task(self,list_info,iNumber):
        # 存放1个队列处理的结构字典 
        tmp_dir = {}
        # 字典的最后一个数据的时间戳
        time_end_possible = 0
        # 写入文件的字符串
        szWriteFlux = ""
        szWriteFlow = ""
        try:
            # 整合数据 
            self.integrated_date(list_info,tmp_dir,iNumber)
            # 字典的长度
            iDirNumber = len(tmp_dir)
            # 遍历字典次数
            iTmpNumber = 0
            # 遍历字典
            for fluxkey,fluxvalue in tmp_dir.iteritems():
                # 把string转换成字典
                fluxbase = loads(fluxkey)
                iTmpNumber += 1
                if iDirNumber <= iTmpNumber:
                    # 设置队列数据结束的时间戳
                    time_end_possible = fluxbase["timestmap"]
                # 时间戳，域名，节点，地区，动静态，带宽，访问次数，访问时间
                szWriteFlux = szWriteFlux+"%ld\t%s\t%s\t%s\t%c\t%ld\t%d\t%ld\t\n" %(
                                                                        fluxbase["timestmap"],
                                                                        fluxbase["domain"],
                                                                        fluxbase["cacheip"],
                                                                        fluxbase["area"], 
                                                                        fluxbase["ds"],
                                                                        (int((fluxvalue[0] + 299) / 300)),
                                                                        fluxvalue[1],
                                                                        fluxvalue[2])
                # 时间戳，域名，地区，动静态，流量
                szWriteFlow = szWriteFlow+"%ld\t%s\t%s\t%c\t%ld\t\n" %(
                                                                        fluxbase["timestmap"],
                                                                        fluxbase["domain"],
                                                                        fluxbase["area"], 
                                                                        fluxbase["ds"],
                                                                        int((fluxvalue[0]))) 
            #写入flux文件
            self.write_flux(iNumber,szWriteFlux,time_end_possible)
            # 清空字符串
            szWriteFlux = ""
        
            #写入flow文件
            self.write_flow(iNumber,szWriteFlow,time_end_possible)
        except Exception as ex:
            self.log.error("fivemin_truck-progress_task error:%s",str(ex))
        # 清空字符串
        szWriteFlow = ""
        #清空字典
        tmp_dir.clear()
        return 0
    
    # 整合数据
    def integrated_date(self,list_info,tmp_dir,iNumber):
        # 读取行数的次数 
        #ilinetimes = 0
        for line in list_info:
            '''
            # 读取行数后适当的sleep，让处理协程有机会执行 
            ilinetimes += 1
            if ilinetimes > 1024:
                ilinetimes = 0
                gevent.sleep(0.01)
            '''
            try:
                # url分割标志 
                szUrlIndex = "/"
                # 一行的数据分别赋值 
                time_stamp,request_Url,client_ip,chche_code,cache_size,last_time,hier_code = line
                # 时间戳转换成int类型
                timestmap = int(string.atof(time_stamp))
                # url处理，提取域名
                Url_List = request_Url.split(szUrlIndex)
                # URL不符合格式 
                if(len(Url_List) < 3):
                    continue
                    #print "section_date request_url is wrong-URL len is less than 3"
                else:
                    domain = Url_List[2]
                # 处理不符合要求的域名以及URL
                if (255 < len(Url_List)) or (2 >= len(domain)):
                    continue
                # 节点ip
                cacheip = self.config.BING_IP[iNumber]
                # 查找地区
                fivemin_gl.view_name_sem.acquire()
                area = find_view_area(client_ip)
                fivemin_gl.view_name_sem.release()
                # 动静态
                if ("NONE" in chche_code) or ("HIT" in chche_code) or ("HIT" in hier_code):
                    ds = "S"
                else:
                    ds = "D"
                # 流量
                cache_size = int(string.atof(cache_size)) 
                # 访问时间
                meet_time = int(string.atof(last_time))  
                # 字典的Key值 
                key = {"timestmap":timestmap,"domain":domain,"cacheip":cacheip,"area":area,"ds":ds}
                stringkey = dumps(key) 
                value = [cache_size,1,meet_time]
                # 判断是否在字典中
                if stringkey in tmp_dir:
                    # 原有的数据金额现有的数据相结合 
                    new_value = [x+y for x,y in zip(value,tmp_dir[stringkey])]
                    tmp_dir[stringkey] = new_value
                else:
                    tmp_dir[stringkey] = value
            except Exception as ex:
                self.log.error("fivemin_truck[%d]-integrated_date error:%s",self.threadID,str(ex))
                self.log.error("integrated_date:%s",line)

    # 写入带宽文件
    def write_flux(self,iNumber,szWriteFlux,time_end_possible):
        # flux 的名字
        szflux_path_name = get_fluxname(self.config.BING_IP[iNumber])
        # flux最终的路径
        szflux_path = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLUXPATH"],szflux_path_name)
        # flux在写入中的路径
        szflux_tmppath = "%s/flux.tmp%d.gz" %(self.config.SERVICE_LIST[iNumber]["FLUXPATH"],iNumber)
        
        fivemin_gl.flux_fd_sem.acquire()
        try:
            # 创建临时写入数据文件 
            if not fivemin_gl.flux_fd[iNumber]:
                fivemin_gl.flux_fd[iNumber] = gzip.open(szflux_tmppath, "ab+")
            # 写入文件
            fivemin_gl.flux_fd[iNumber].write(szWriteFlux)
       
            # 5分钟截断文件
            if time_end_possible - fivemin_gl.start_time[iNumber] >= 300:
                self.log.debug("fivemin_truck-write_flux-write %s OK",szflux_path_name)
                fivemin_gl.flux_fd[iNumber].close()
                os.rename(szflux_tmppath,szflux_path)
                # flow文件写入的时候在处理  
                fivemin_gl.flux_fd[iNumber] = 0
        except Exception as ex:
            self.log.error("fivemin_truck[%d]-write_flux error:%s",self.threadID,str(ex))
        fivemin_gl.flux_fd_sem.release()
        
    # 写入流量文件
    def write_flow(self,iNumber,szWriteFlow,time_end_possible):
        # flux 的名字
        szflow_path_name = get_flowlogname(self.config.BING_IP[iNumber])
        # flow最终的路径
        szflow_path = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],szflow_path_name)
        # flow在写入中的路径
        szflux_tmppath = "%s/flow.tmp%d.log" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],iNumber)
        
        fivemin_gl.flow_fd_sem.acquire()
        try:
            if not fivemin_gl.flow_fd[iNumber]:
                fivemin_gl.flow_fd[iNumber] = open(szflux_tmppath, "a+")
            # 写入文件
            fivemin_gl.flow_fd[iNumber].write(szWriteFlow)
            #print "write szWriteFlow %d" %(len(szWriteFlow))
            # 5分钟截断文件
            if time_end_possible - fivemin_gl.start_time[iNumber] >= 300:
                self.log.debug("fivemin_truck-write_flux-write %s OK",szflow_path_name)
                fivemin_gl.flow_fd[iNumber].close()
                fivemin_gl.start_time[iNumber] = time_end_possible
                # 截断文件
                os.rename(szflux_tmppath,szflow_path)
                fivemin_gl.flow_fd[iNumber] = 0
        except Exception as ex:
            self.log.error("fivemin_truck[%d]-write_flow error:%s",self.threadID,str(ex))
        fivemin_gl.flow_fd_sem.release()
        
        
