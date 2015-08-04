#! /usr/bin/python
#-*- coding: utf-8 -*-

import os,string,re,uuid,time
from fivemin_gl import view_name_list

'''
    get uuid number based on timestamp
'''
def get_uuid():
    szUUID = string.replace(str(uuid.uuid4()),"-","")
    return szUUID.upper()

'''
    get Raw Log name
'''
def get_logname(ip):
    szName = "%s-%s-%lds1.gz" %(ip,get_uuid(),time.time())
    return szName

'''
    get flux file name
'''
def get_fluxname(ip):
    szName = "%s_%s_%ld_apflux.gz" %(ip,get_uuid(),time.time())
    return szName

'''
    get flow file name
'''
def get_flowlogname(ip):
    szName = "%s_%s_%ld_apflow.log" %(ip,get_uuid(),time.time())
    return szName

'''
    get flow file name
'''
def get_flowname(ip):
    sztime_date = time.strftime("%Y%m%d")
    szName = "%s_%s_%s_apflow.gz" %(ip,get_uuid(),sztime_date)
    return szName

'''
    get local ip
'''
def get_ip_address():
    ip = os.popen("/sbin/ifconfig | grep 'inet addr' | awk '{print $2}'").read()
    ip = ip[ip.find(':')+1:ip.find('\n')]
    return ip

'''
    check whether the ip is legal 
'''
def checkip(ip):
    p = re.compile('^(([01]?\d\d?|2[0-4]\d|25[0-5])\.){3}([01]?\d\d?|2[0-4]\d|25[0-5])$')
    if p.match(ip):
        return True
    else:
        return False 

'''
    change ip into int
'''
def ip_into_int(ip):
    return reduce(lambda x,y:(x<<8)+y,map(int,ip.split('.')))

'''
    check whether ip is belong to network
'''    
def is_same_network(ip, network): 
    network = network.split('/') 
    mask = ~(2**(32-int(network[1])) - 1) 
    return (ip_into_int(ip) & mask) == (ip_into_int(network[0]) & mask) 

# 查找IP在哪个区域
def find_view_area(ip):
    for iNum in xrange(len(view_name_list)):
        # 把ip和掩码与的位运算 
        ifind = ip_into_int(ip) & view_name_list[iNum].mask_t
        # 查找ip是否在字典中 
        if ifind in view_name_list[iNum].netid_t:
            return view_name_list[iNum].netid_t[ifind]
    return "qita"
