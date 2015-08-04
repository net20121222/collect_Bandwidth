#! /usr/bin/python
#-*- coding: utf-8 -*-

'''
    example:anhui-huangshan-tietong { match-clients {110.122.224.0/20
    area list:subnet mask(at most 32),dirc of ip(key is 110.122.224.0,value is anhui-huangshan-tietong)
'''      
class ViewName:
    def __init__(self):
        self.mask_t = 0
        self.netid_t = {}
'''
flux file need 5 property :timestamp,domain,cache_ip,area,dynamic or static
'''
class Flux_Base:
    def __init__(self):
        self.timestamp = 0
        self.domain = ""
        self.cache_ip = ""
        self.area = ""
        self.ds = "S"
        self.level = 0
'''
flux file need 3 property :flux,Number of visits，keep visiting time
'''
class Flux_Progress:
    def __init__(self):
        self.flux = 0
        self.visited = 0
        self.meet_time = 0
'''
    36 field  of squid/ats 
'''
class data:
    def __init__(self):
        # ip adddress
        self.client_ip = "127.0.0.0"
        self.timestamp = ""
        self.major_version = ""
        self.monir_version = ""
        self.response_code = ""
        self.cache_code = ""
        self.cache_size = ""
        
        self.http_method = ""
        self.request_url = ""
        self.code_rate = ""
        self.channel = ""
        self.qq_num = ""
        self.hier_code = ""
        self.server_ip = ""
        self.file_type = ""
        self.reffer = ""
        self.user_agent = ""
        self.last_time = ""

        self.user1 = ""
        self.user2 = ""
        self.cache_ip = ""
        self.time_out = ""
        self.cookie = ""
        self.total_time = ""
        self.uplayer_time = ""
        self.dns_time = ""
        self.client_port = ""
        self.is_normal_closed = ""
        self.range = ""

        self.reserved1 = ""
        self.reserved2 = ""
        self.reserved3 = ""
        self.reserved4 = ""
        self.reserved5 = ""
        self.reserved6 = ""
        self.reserved7 = ""