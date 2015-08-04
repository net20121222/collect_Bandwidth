#! /usr/bin/python
#-*- coding: utf-8 -*-

import os,time,socket,stat,re,gzip
from json import loads,dumps
from fivemin_common import get_flowname
from fivemin_gl import bStopPro
import gevent.monkey
gevent.monkey.patch_all() 

class client():
    def __init__(self,config,log):
        # 配置文件 
        self.config = config
        # log文件 
        self.log = log

    def handle_task(self):
        # 删除过期文件的时刻 
        tDelete = 0
        # 合并流量的时刻
        t_flow_day = 0
        while 1:
            tNow = time.time()
            # 合并流量的时机判断 
            t_flow_now = time.localtime()
            # 定期删除文件 
            if (tNow - tDelete) > 24 * 60 * 60:
                self.delete_path(self.config.SERVICE_LIST[iNumber]["FLUXPATH"])
                self.delete_path(self.config.SERVICE_LIST[iNumber]["LOGPATH"])
                self.delete_path(self.config.SERVICE_LIST[iNumber]["FLOWPATH"])
                tDelete = tNow
            # 合并流量时间是00:01~00:08 
            if ((t_flow_day != t_flow_now.tm_mday) and (t_flow_now.tm_hour == 0) and ((t_flow_now.tm_min - 1) < 8)):
                for i in range(self.config.FILE_NAME):
                    # 开始合并流量
                    if self.merg_flow(i) < 0:
                        self.log.error("fivemin_upfile-handle_task-merg_flow failed")
                # 刷新合并流量的时间 
                t_flow_day = t_flow_now.tm_mday
            # 开始上传文件（包括原始日志，流量文件，带宽文件） 
            for j in range(self.config.FILE_NAME):    
                self.send_file(j)
            # 退出程序通知
            if bStopPro[0]:
                # 设置上传功能正常停止
                bStopPro[1] = True
                self.log.debug("fivemin_upfile-handle_task-upload exit")
                break
            gevent.sleep(10)
        return 0

    # 删除过期的文件
    def delete_path(self,iNumber):
        # 现在时刻
        tNow = time.time()
        # 获取文件列表
        file_list = os.listdir(path)
        for file_name in file_list:
            # 删除文件的绝对路径
            szPath = "%s/%s" %(path,file_name)
            try:
                # 文件最后修改时刻
                tfile = os.path.getmtime(szPath)
                # 存在时间大于一周
                if tNow - tfile > 72*60*60:
                    # 删除文件
                    os.remove(szPath)i
            except OSError as ex:
                self.log.error("fivemin_upfile-delete_path:%s",str(ex))

    # 上传文件
    def send_file(self,iNumber):
        # 上传带宽文件
        self.log.debug("===================start to up flux======================")
        if self.send_flux(iNumber) < 0:
            self.log.error("start to up flux failed")
        else:
            self.log.debug("start to up flux succeed")
        # 上传原始日志    
        self.log.debug("===================start to up backlog======================")        
        if self.send_backlog(iNumber) < 0:
            self.log.error("start to up backlog failed")
        else:
            self.log.debug("start to up backlog succeed")
        # 上传流量    
        self.log.debug("===================start to up flow======================")
        if self.send_flow(iNumber) < 0:
            self.log.error("start to up flow failed")
        else:
            self.log.debug("start to up flow succeed")
    
    # 上传带宽文件
    def send_flux(self,iNumber): 
        # 上传文件的标志位是带宽
        szflux_flag = "FLUX"
        # 上传带宽日志的目录
        if self.load_file_path(iNumber,self.config.SERVICE_LIST[iNumber]["FLUXPATH"],szflux_flag) < 0:
            return -1
        return 0
    
    # 上传原始日志文件
    def send_backlog(self,iNumber):
        # 上传文件的标志位是原始日志
        szbacklog_flag = "UPLOAD"
        # 上传原始日志的目录
        if self.load_file_path(iNumber,self.config.SERVICE_LIST[iNumber]["LOGPATH"],szbacklog_flag) < 0:
            return -1
        return 0
    
    # 上传流量文件
    def send_flow(self,iNumber):
        # 上传文件的标志位是流量
        szflux_flag = "FLOW"
        # 上传流量日志的目录
        if self.load_file_path(iNumber,self.config.SERVICE_LIST[iNumber]["FLOWPATH"],szflux_flag) < 0:
            return -1
        return 0
    
    # 合并流量
    def merg_flow(self,iNumber):
        self.log.debug("===================start merg_flow======================")
        # 流量log文件list 
        list_filename = os.listdir(self.config.SERVICE_LIST[iNumber]["FLOWPATH"])
        # 流量处理的字典结构 
        tmp_flow_dir = {}
        for filename in list_filename:
            # 过滤处理中的.log流量文件 
            if re.search('apflow.log', filename):
                # 流量文件的绝对路径
                file_path = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],filename)
                try:
                    with open(file_path) as file_fd:
                        for line in file_fd:
                            # 去除行末尾的\t\n,以\t为分隔符转化为list
                            line_list = line.rstrip('\t\n').split('\t')
                            # 赋值 
                            time_stamp,domain,area,ds,flux_value = line_list
                            # 把时间戳转化为年月日 
                            time_tmp = time.localtime(int(time_stamp))
                            time_date = time.strftime('%Y%m%d',time_tmp)
                            # 字典的Key值 
                            key = {"time_date":time_date,"domain":domain,"area":area,"ds":ds}
                            # 将字典转换为字符串
                            string_key = dumps(key)
                            # 流量
                            value = int(flux_value)
                            # 检查是否存在相同的数据 
                            if string_key in tmp_flow_dir:
                                tmp_flow_dir[string_key] += value
                            else:
                                tmp_flow_dir[string_key] = value
                except IOError as ex:
                    self.log.error("fivemin_upfile-merg_flow-error in merg_flow:%s",str(ex))
                    continue
                os.remove(file_path)
        # 手动清空list 
        list_filename = []
        # 写入文件
        if self.write_upflow(tmp_flow_dir) < 0:
            # 清空字典
            tmp_flow_dir.clear()
            return -1
        # 清空字典
        tmp_flow_dir.clear()
        self.log.debug("===================merg_flow succeed======================")
        return 0
   
    # 写入流量文件
    def write_upflow(self,tmp_flow_dir,iNumber):
        # 写入流量文件的buff
        szWriteFlow = ""
        szflow_name = get_flowname(self.config.BING_IP[iNumber])
        # 流量文件的绝对路径
        szflow_path = "%s/%s" %(self.config.SERVICE_LIST[iNumber]["FLOWPATH"],szflow_name)
        # 写入文件描述符
        Wri_flow_fd = gzip.open(szflow_path, "ab+")
        try :
            # 遍历字典 
            for flowkey,flowvalue in tmp_flow_dir.iteritems():
                # 把string结构结构转换成字典结构
                flowbase = loads(flowkey)
                # 日期，域名，地区，动静态，流量
                szWriteFlow = szWriteFlow+"%s\t%s\t%s\t%c\t%ld\t\n" %(
                                                                    flowbase["time_date"],
                                                                    flowbase["domain"],
                                                                    flowbase["area"], 
                                                                    flowbase["ds"],
                                                                    flowvalue)
                # 40K写入一次 
                if len(szWriteFlow) > 1024*40:
                    Wri_flow_fd.write(szWriteFlow)
                    szWriteFlow = ""
            if len(szWriteFlow) > 0:
                Wri_flow_fd.write(szWriteFlow)
                # 清空buff 
                szWriteFlow = ""
        except IOError as ex:
            self.log.error("fivemin_upfile-merg_flow-write file error:%s",str(ex))
            return -1
        finally:
            Wri_flow_fd.close()
        return 0

    def load_file_path(self,iNumber,szPath,szsend_flag):
        # 需要上传的文件list
        szPath_list = os.listdir(szPath)
        if not szPath_list:
            return 0
        # 链接上产的服务器
        fd = self.login(iNumber,szsend_flag)
        if fd < 0:
            return -1
        for tmpPath in szPath_list:
            # 过滤文件格式 
            if re.search('apflow.gz', tmpPath) or re.search('apflux.gz', tmpPath) or re.search('s1.gz', tmpPath) or re.search('s0.gz', tmpPath):
                # 上传文件
                if self.upload_file(fd,tmpPath,szPath) < 0:
                    fd.close()
                    return -1
                    '''
                    if login(iNumber,szsend_flag) < 0:
                    return -1
                    continue
                    '''
                else:
                    # 删除上传成功文件
                    tmpFilePath = "%s/%s" %(szPath,tmpPath)
                    os.remove(tmpFilePath)
        fd.close()
        return 0

    def login(self,iNumber,szsend_flag):
        # 上传服务器，根据上传的文件类型，设置不同的用户，密码，ip，端口
        tmpsend_flag = "%s%s" %(szsend_flag,"NAME")
        IpAddress = self.config.UPLOAD_LIST[iNumber][tmpsend_flag]
        tmpsend_flag = "%s%s" %(szsend_flag,"PORT")
        IpPort = self.config.UPLOAD_LIST[iNumber][tmpsend_flag]
        tmpsend_flag = "%s%s" %(szsend_flag,"USER")
        UserName = self.config.UPLOAD_LIST[iNumber][tmpsend_flag]
        tmpsend_flag = "%s%s" %(szsend_flag,"PASSWD")
        UserPassWD = self.config.UPLOAD_LIST[iNumber][tmpsend_flag]
        # 链接服务器
        sockfd = self.connect_Service(IpAddress,IpPort)
        if sockfd < 0:
            return -1
        # 用户密码验证
        szWritebuf = "LOGIN:%s;%s\r\n" %(UserName,UserPassWD)
        iwriteResult = self.write_line(sockfd,szWritebuf,len(szWritebuf))
        if iwriteResult < 0:
            sockfd.close()
            return -1
        # 读取用户密码验证结果
        szreadlist = []
        if self.read_line(sockfd,szreadlist,128) < 0:
            sockfd.close()
            return -1
        readbuf = ''.join(szreadlist)
        # 验证结果
        if cmp(readbuf[0:3],"+OK"):
            sockfd.close()
            return -1    
        return sockfd
    
    # 链接服务器
    def connect_Service(self,IpAddress,IpPort):
        # 创建socket
        try:
            sockfd = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        except socket.error, value:
            (errno,err_msg) = value
            self.log.error("fivemin_upfile-connect_Service-creat socket failed: %s, errno=%d",err_msg, errno)
            return -1
        # 链接服务器
        try:
            sockfd.connect((IpAddress,IpPort))
        except socket.error, value:
            (errno,err_msg) = value
            self.log.error("fivemin_upfile-connect_Service-connect socket failed: %s, errno=%d",err_msg, errno)
            sockfd.close()
            return -1
        self.log.debug("fivemin_upfile-connect_Service-connect: %s succeed",IpAddress)
        return sockfd 
    
    # 写入socket    
    def write_line(self,fd,buf,size=1024):
        # 写入sock的字节数
        sendsize = 0
        while sendsize < size:
            write = fd.send(buf[sendsize:])
            sendsize += write
        return sendsize
    
    # 读取socket 
    def read_line(self,fd,buf,size=1024):
        # 读取sock的字节数
        readsize = 0
        while readsize < size:
            date = fd.recv(size)
            readsize += len(date)
            buf.append(date)
            # 服务器端回复以\r\n为结束标志
            if date.find("\r\n"):
                break    
        return readsize 
    
    # 上传文件 
    def upload_file(self,fd,tmpPath,szPath):
        # 上传的文件绝对路径
        tmpFilePath = "%s/%s" %(szPath,tmpPath)
        # 查询文件状态
        file_stat = os.stat(tmpFilePath)
        if None != file_stat:       
            Buff = "%s;%d\r\n" %(tmpPath,(file_stat[stat.ST_SIZE]))
        # 发送文件的大小以及上传的文件名
        if self.write_line(fd,Buff,len(Buff)) < 0:
            fd.close()
            return -1
        # 读取服务器端回应
        szreadlist = []
        if self.read_line(fd,szreadlist,128) < 0:
            fd.close()
            return -1
        readbuf = ''.join(szreadlist)
        # 判断服务器端回应结果 
        if cmp(readbuf[0:3],"+OK"):
            fd.close()
            return -1
        # 开始读取文件传输
        try:
            with open(tmpFilePath,'r') as up_file_fd:
                date = ""
                while 1:
                    # 读取10K内容
                    date = up_file_fd.read(10240)
                    # 内容空了跳出循环 
                    if not date:
                        break
                    # 写入socket
                    if self.write_line(fd,date,len(date)) < 0:
                        return -1
                    # 每次写入后清空date
                    date = ""
        except IOError as err:
            self.log.error("fivemin_upfile-upload_file-open error %s",str(err))
            return -1
        # 读取service端确认结果 
        szreadlist = []
        if self.read_line(fd,szreadlist,128) < 0:
            fd.close()
            return -1
        readbuf = ''.join(szreadlist)
        # 判断是否上传完毕
        if cmp(readbuf[0:3],"+OK"):
            fd.close()
            return -1
        self.log.debug("fivemin_upfile-upload_file-upload file succeed")
        return 0          

    
if __name__ == "__main__":
    pass
