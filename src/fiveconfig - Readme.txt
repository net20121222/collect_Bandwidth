[ROOT]
TRUNC_PROCESS_DISABLE = 0 是否禁止启动处理服务0：不禁止，1：禁止
SEND_PROCESS_DISABLE = 0 是否禁止启动上传服务0：不禁止，1：禁止
UPDATE_PROCESS_DISABLE = 1 是否禁止启动更新服务0：不禁止，1：禁止（暂时无法使用）
FILE_NAME = 2 监视Ip个数
IPNAMEDPATH = named.conf IP归属文件名字

[SERVICE1]
INPATH = /home/test/access.log 处理LOG的绝对路径
OUTPATH = backlog 5分钟处理LOG的绝对路径
LOGPATH = fluxlog 5分钟处理LOG生成原始日志的路径
FLUXPATH = flux 5分钟处理LOG生成带宽日志的路径
FLOWPATH = flow 5分钟处理LOG生成流量日志的路径
URLPATH = push 5分钟处理LOG生成推送文件的路径
SERVICECONFPATH = /usr/local/squid/etc/squid.conf 服务的配置文件的绝对路径
SERVICESWAPPATH = /cache/cache/swap.state 服务的缓存文件的绝对路径
EXTRAURL = 1 是否禁止启动url推送服务0：不禁止，1：禁止 （暂时功能未实现）
DNAMERULE = 0 域名分割等级 1级域名：0：（暂时无用）
DATAFORMAT = 0 服务的类型 squid：0


[SERVICE2]
INPATH = /home/test/access1.log 处理LOG的绝对路径
OUTPATH = backlog 5分钟处理LOG的绝对路径
LOGPATH = fluxlog 5分钟处理LOG生成原始日志的路径
FLUXPATH = flux 5分钟处理LOG生成带宽日志的路径
FLOWPATH = flow 5分钟处理LOG生成流量日志的路径
URLPATH = push 5分钟处理LOG生成推送文件的路径
SERVICECONFPATH = /usr/local/squid/etc/squid.conf.2 服务的配置文件的绝对路径
SERVICESWAPPATH = /cache/cache/swap.state 服务的缓存文件的绝对路径
EXTRAURL = 1 是否禁止启动url推送服务0：不禁止，1：禁止 （暂时功能未实现）
DNAMERULE = 0 域名分割等级 1级域名：0：（暂时无用）
DATAFORMAT = 0 服务的类型 squid：0

[UPLOAD1]
UPLOADNAME = 118.244.210.5 原始日志上传IP
UPLOADPORT = 10533 原始日志上传端口
UPLOADUSER = up_user 原始日志上传用户名
UPLOADPASSWD = up_password 原始日志上传密码

FLUXNAME = 118.244.210.5 带宽日志上传IP
FLUXPORT = 10533 带宽日志上传端口
FLUXUSER = up_user 带宽日志上传用户名
FLUXPASSWD = up_password 带宽日志上传密码

FLOWNAME = 118.244.210.8 流量日志上传IP
FLOWPORT = 10546 流量日志上传端口
FLOWUSER = up_uselr 流量日志上传用户名
FLOWPASSWD = up_passworld 流量日志上传密码
 
[UPLOAD2]
UPLOADNAME = 118.244.210.5 原始日志上传IP
UPLOADPORT = 10533 原始日志上传端口
UPLOADUSER = up_user 原始日志上传用户名
UPLOADPASSWD = up_password 原始日志上传密码

FLUXNAME = 118.244.210.5 带宽日志上传IP
FLUXPORT = 10533 带宽日志上传端口
FLUXUSER = up_user 带宽日志上传用户名
FLUXPASSWD = up_password 带宽日志上传密码

FLOWNAME = 118.244.210.8 流量日志上传IP
FLOWPORT = 10546 流量日志上传端口
FLOWUSER = up_uselr 流量日志上传用户名
FLOWPASSWD = up_passworld 流量日志上传密码

