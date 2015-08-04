#! /usr/bin/python
#-*- coding: utf-8 -*-

import os
import ConfigParser,re

class Config():
    def __init__(self,log):
        # 是否禁用处理进程
        self.TRUNC_PROCESS_DISABLE = 0
        # 是否禁用发送进程
        self.SEND_PROCESS_DISABLE = 0
        # 是否禁用升级进程
        self.UPDATE_PROCESS_DISABLE = 1
        # 处理IP个数
        self.FILE_NAME = 0
        # name。conf的目录
        self.IPNAMEDPATH = ""
        # 监听的service内数据
        self.SERVICE_LIST = []
        # upload内数据
        self.UPLOAD_LIST = []
        # 绑定的IP
        self.BING_IP = []
        # log记录
        self.log = log
        # squid文件缓存文件
        self.SQUID_SER_CACHE = []

    def read_config(self):
        config = ConfigParser.ConfigParser()
        szCurPath = os.getcwd()
        szfilePath = os.path.dirname(szCurPath)
        # 5分钟的配置文件位置 
        szPath = "%s/fiveconfig.ini" %(szCurPath)
        # 读取配置文件
        if os.path.exists(szPath):
            with open(szPath,'rb') as iConfigfd:
                try :
                    # 读取配置文件 
                    config.readfp(iConfigfd)
                    # 处理功能是否开启 
                    self.TRUNC_PROCESS_DISABLE = config.getint("ROOT", "TRUNC_PROCESS_DISABLE")
                    # 上传功能是否开启 
                    self.SEND_PROCESS_DISABLE = config.getint("ROOT", "SEND_PROCESS_DISABLE")
                    # 升级进程是否开启 
                    self.UPDATE_PROCESS_DISABLE = config.getint("ROOT", "UPDATE_PROCESS_DISABLE")
                    # 监听IP的个数 
                    self.FILE_NAME = config.getint("ROOT", "FILE_NAME")
                    # named.conf配置文件目录 
                    self.IPNAMEDPATH = "%s/%s" %(szCurPath,config.get("ROOT", "IPNAMEDPATH"))
                    
                    # 轮询读取绑定的IP的配置内容
                    for i in xrange(self.FILE_NAME):
                        szServicePart = "SERVICE%d" %(i+1)
                        # 配置文件中Service的内容 
                        tmpServiceDir = {}
                        # 配置文件中上传的内容 
                        tmpUploadDir = {}
                        # access.log的绝对路径 
                        tmpServiceDir["INPATH"] = config.get(szServicePart, "INPATH")
                        # 读取access.log的处理文件绝对路径 
                        tmpServiceDir["OUTPATH"] = "%s/%s" %(szfilePath,config.get(szServicePart, "OUTPATH"))
                        if not os.path.isdir(tmpServiceDir["OUTPATH"]):
                            os.mkdir(tmpServiceDir["OUTPATH"])  
                        # 原始日志存放的绝对路径 
                        tmpServiceDir["LOGPATH"] = "%s/%s" %(szfilePath,config.get(szServicePart, "LOGPATH"))
                        if not os.path.isdir(tmpServiceDir["LOGPATH"]):
                            os.mkdir(tmpServiceDir["LOGPATH"])  
                        # 带宽文件存放的绝对路径 
                        tmpServiceDir["FLUXPATH"] = "%s/%s" %(szfilePath,config.get(szServicePart, "FLUXPATH"))
                        if not os.path.isdir(tmpServiceDir["FLUXPATH"]):
                            os.mkdir(tmpServiceDir["FLUXPATH"]) 
                        # 流量文件存放的绝对路径 
                        tmpServiceDir["FLOWPATH"] = "%s/%s" %(szfilePath,config.get(szServicePart, "FLOWPATH"))
                        if not os.path.isdir(tmpServiceDir["FLOWPATH"]):
                            os.mkdir(tmpServiceDir["FLOWPATH"])
                        # 推送文件存放的绝对路径 
                        tmpServiceDir["URLPATH"] = "%s/%s" %(szfilePath,config.get(szServicePart, "URLPATH"))
                        if not os.path.isdir(tmpServiceDir["URLPATH"]):
                            os.mkdir(tmpServiceDir["URLPATH"])
                        # 加速软件的配置文件的绝对路径 
                        tmpServiceDir["SERVICECONFPATH"] = config.get(szServicePart, "SERVICECONFPATH")
                        # 加速软件的缓存文件 
                        tmpServiceDir["SERVICESWAPPATH"] = config.get(szServicePart, "SERVICESWAPPATH")
                        # 是否开启Url推送服务 
                        tmpServiceDir["EXTRAURL"] = config.getint(szServicePart, "EXTRAURL")
                        # 域名切割等级 
                        tmpServiceDir["DNAMERULE"] = config.getint(szServicePart, "DNAMERULE")
                        # 加速软件类型 
                        tmpServiceDir["DATAFORMAT"] = config.getint(szServicePart, "DATAFORMAT")
                        self.SERVICE_LIST.append(tmpServiceDir)
                        
                        szServicePart = "UPLOAD%d" %(i+1)
                        # 上传原始日志的用户，密码，ip地址，端口 
                        tmpUploadDir["UPLOADNAME"] = config.get(szServicePart, "UPLOADNAME")
                        tmpUploadDir["UPLOADPORT"] = config.get(szServicePart, "UPLOADPORT")
                        tmpUploadDir["UPLOADUSER"] = config.get(szServicePart, "UPLOADUSER")
                        tmpUploadDir["UPLOADPASSWD"] = config.get(szServicePart, "UPLOADPASSWD")
                        tmpUploadDir["FLUXNAME"] = config.get(szServicePart, "FLUXNAME")
                        # 上传带宽文件的用户，密码，ip地址，端口 
                        tmpUploadDir["FLUXPORT"] = config.get(szServicePart, "FLUXPORT")
                        tmpUploadDir["FLUXUSER"] = config.get(szServicePart, "FLUXUSER")
                        tmpUploadDir["FLUXPASSWD"] = config.get(szServicePart, "FLUXPASSWD")
                        tmpUploadDir["FLOWNAME"] = config.get(szServicePart, "FLOWNAME")
                        # 上传流量文件的用户，密码，ip地址，端口 
                        tmpUploadDir["FLOWPORT"] = config.get(szServicePart, "FLOWPORT")
                        tmpUploadDir["FLOWUSER"] = config.get(szServicePart, "FLOWUSER")
                        tmpUploadDir["FLOWPASSWD"] = config.get(szServicePart, "FLOWPASSWD")
                        self.UPLOAD_LIST.append(tmpUploadDir)
                except IOError as err:
                    self.log.error("Read fiveconfig.ini error:%s",(str(err)))
                    return -1            
        else:
            self.log.error("FiveConfig.ini is not exist")
            return -1
        
        # 轮询Service绑定
        for m in range(self.FILE_NAME):
            # 读取绑定IP
            if self.read_bindIP(m) < 0:
                return -1
            self.log.debug("Bind IP:%s",self.BING_IP[m])
            # squid服务相关配置读取
            if 0 == self.SERVICE_LIST[m]["DATAFORMAT"]:
                # 读取squid文件夹
                if self.read_cache(m) < 0:
                    return -1
        return 0
     
    # 读取绑定的缓存文件
    def read_cache(self,iNumber):
        # 加速服务的配置文件绝对路径 
        szPath  = self.SERVICE_LIST[iNumber]["SERVICECONFPATH"]
        # Service的配置文件
        cache_list = []
        # 查找的关键字
        szfind = "cache_dir"
        # 查找关键字的长度
        ifind_len = len(szfind)
        if not os.path.exists(szPath):
            self.log.error("%s is not exists",szPath)
            return -1
        with open(szPath) as cachefd:
            try:
                for line in cachefd:
                    if line[0] == "#":
                        continue
                    if not cmp(line[:ifind_len],szfind):
                        cache_path_list = re.findall(r'/\w+/\w+',line)
                        if cache_path_list:
                            cache_list.append(cache_path_list[0])
                    continue
            except IOError as ex:
                self.log.error("read ServicePath error:%s",str(ex))
                return -1
        # 获取到cache文件夹
        if cache_list:
            # 保存cache绝对路径
            tmp_list = [path+"/swap.state" for path in cache_list]
            self.SQUID_SER_CACHE.append(tmp_list)
        else:
            self.log.error("Please check squid cache path")
            return -1
        return 0
    
    # 读取绑定的IP
    def read_bindIP(self,iNumber):
        # 查找ip的关键字 
        szfind = ""
        # 加速服务的配置文件绝对路径 
        szPath  = self.SERVICE_LIST[iNumber]["SERVICECONFPATH"]
        # 绑定的IP 
        tmp_bind_ip  = ""
        # 0 is squid service
        if 0 == self.SERVICE_LIST[iNumber]["DATAFORMAT"]:
            # http_port 119.4.115.17:80 transparent
            szfind = "http_port"
        # 1 is ATS service
        elif 1 == self.SERVICE_LIST[iNumber]["DATAFORMAT"]:
            # LOCAL proxy.local.incoming_ip_to_bind STRING 119.4.115.50
            szfind = "LOCAL proxy.local.incoming_ip_to_bind"
        # 查找关键字的长度
        szfindNum = len(szfind)     
        if os.path.exists(szPath):
            with open(szPath) as bindIPfd:
                try:
                    for line in bindIPfd:
                        # 跳过注释文件 
                        if line[0] == "#":
                            continue
                        # 查找文件中德IP
                        if not cmp(line[:szfindNum],szfind):
                            tmp_list = re.findall(r'\d+.\d+.\d+.\d+',line)
                            if tmp_list:
                                tmp_bind_ip = tmp_list[0]
                            break
                        else:
                            continue
                except:
                    self.log.error("read_bindIP error")
                    return -1
        else:
            self.log.error("SERVICECONFPATH%d is not exist",iNumber)
            # 获取主机IP 
            self.BING_IP.append(get_ip_address())
            return -1
        # 没有获取IP
        if not tmp_bind_ip:
            # 设置本地的IP 
            self.BING_IP.append(get_ip_address())
        else:
            # 设置绑定的IP 
            self.BING_IP.append(tmp_bind_ip)
        return 0
                       
# 获取本机IP
def get_ip_address():
    ip = os.popen("/sbin/ifconfig | grep 'inet addr' | awk '{print $2}'").read()
    ip = ip[ip.find(':')+1:ip.find('\n')]
    return ip
            
if __name__ == "__main__":
    list  =[1]
    if list:
        print 1
    '''
    fivemin_config = Config()
    fivemin_config.read_config()
    print fivemin_config.SERVICE_LIST
    print fivemin_config.UPLOAD_LIST
    print fivemin_config.BING_IP
    '''
