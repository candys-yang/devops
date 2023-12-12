# 备份 Mysql数据库 脚本
# by: yaokai	2023-12
#
#   每次执行脚本时，会使数据库产生一个新的 binlog 日志文件
#   每天 00:00 执行时，将进行全备。其他时间执行将进行增量备份
#   
#
#   环境配置 /etc/my.cnf ，添加 client 配置：
#       [client]
#       user = root
#       password = xxxxxxxxxxxxx
#
#
#
#   # 脚本只使用python标准库
#   python3 mysql_backup.py 
#   # 定时任务 crontab 
#   0 */4 *   *   *     cd /root/mysqlbackup/ && python3 /root/mysqlbackup/mysql_backup.py
#   
#   # 执行后的效果：
#       @debug222:mysqlbackup# pwd
#       /root/mysqlbackup
#       @debug222:mysqlbackup# ll
#       drwxr-xr-x  2 root root     4096 Dec 11 20:09 ./
#       drwx------ 50 root root     4096 Dec 11 19:39 ../
#       -rw-r--r--  1 root root 94472176 Dec 11 00:00 backup_full_1702200000
#       -rw-r--r--  1 root root      303 Dec 11 20:07 backup_incremental_1702276701_mysql-bin.000001
#       -rw-r--r--  1 root root      303 Dec 11 19:48 backup_incremental_1702277071_mysql-bin.000002
#       -rw-r--r--  1 root root      307 Dec 11 19:48 backup_incremental_1702277144_mysql-bin.000003
#
#   # 文件名以 _ 分割字段
#   #   full 表示全备，incremental 表示增量备份
#   #   备份类型的下一段为备份时间戳（full 使用 dump 完成后的时间，incremental 使用binlog文件创建时间）
#

import logging 
import os, sys
import time
import datetime
import subprocess
import zipfile

logging.basicConfig(level=logging.DEBUG)
DATE = datetime.datetime.now()
BACKUP_TYPE = 0
BINLOG_PATH = '/var/lib/mysql/'
BINLOG_PREFIX = 'mysql-bin'
EXPIRE_TIME = 3600 * 48      # s

# 备份类型选择
if DATE.hour == 0 and DATE.minute == 0: 
    logging.info("本次备份类型，全量备份。")
    BACKUP_TYPE = 0
else: 
    BACKUP_TYPE = 1
    logging.info("本次备份类型，增量备份。")

#BACKUP_TYPE = 0 #debug 用的变量


def ReadBinlogIndex(): 
    ''' 读取mysql的binlog文件 '''
    binlog_file = []
    if os.path.exists(BINLOG_PATH + BINLOG_PREFIX + '.index') is False: 
        return binlog_file
    with open(BINLOG_PATH + BINLOG_PREFIX + '.index') as f: 
        lines = f.readlines()
        for i in lines: 
            fn = i.strip().replace('./','')
            ct = os.path.getctime(BINLOG_PATH + fn)
            binlog_file.append({
                'filename': fn, 
                'createtime': int(ct) })
    return binlog_file


# 执行全备过程
if BACKUP_TYPE == 0: 
    # 读取binlog文件列表
    binlog_file = ReadBinlogIndex()
    #
    if binlog_file == []: 
        logging.error('binlog index 中没有 binlog 文件记录')
        exit()
    # 
    last_binlog = binlog_file[-1]['filename']

    # 导出数据
    mydump_p = subprocess.Popen(
        "mysqldump -uroot --single-transaction --master-data=1 --flush-logs --triggers --all-databases > dump.sql",
        shell=True, 
        stdout=subprocess.PIPE, 
        stderr=subprocess.PIPE
        )
    mydump_p.communicate()
    if str(mydump_p.returncode) == "0": 
        logging.info("导出数据成功。")
    else: 
        logging.error("导出数据失败，结束程序。")
        exit()
    # 压缩数据
    logging.info("压缩数据...")
    with zipfile.ZipFile('backup_full_' + last_binlog.rsplit('.')[1] + '_' + str(int(time.time())), 'w', zipfile.ZIP_DEFLATED) as zipf: 
        zipf.write("dump.sql", arcname="dump.sql")
    # 删除临时文件
    try: 
        os.remove('dump.sql')
    except OSError as e: 
        logging.error('删除 dump.sql 文件失败,' + str(e))


# 执行增量备份过程
if BACKUP_TYPE == 1: 
    # 刷新 binlog 日志
    mydump_p = subprocess.Popen(
        '/usr/bin/mysql -e "flush logs;"',
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE)
    mydump_p.communicate()
    logging.info('刷新 binlog 日志.')
    if str(mydump_p.returncode) != "0": 
        logging.error('执行 flush logs 失败，备份过程退出。')
        exit()
    # 获取所有 binlog 文件
    binlog_file = ReadBinlogIndex()
    # 复制和压缩增量 binlog 文件
    for i in binlog_file[:-1]: 
        backup_filename = "backup_incremental_" + str(i['createtime']) + '_' + str(i['filename'])
        if os.path.exists(backup_filename) is False: 
            logging.info('复制 binlog: %s', i['filename'])
            with zipfile.ZipFile(backup_filename, 'w', zipfile.ZIP_DEFLATED) as zipf: 
                zipf.write(BINLOG_PATH + i['filename'], arcname=i['filename'])
    # 


# 旧文件清理
for i in os.listdir('./'): 
    try: 
        file_unix = str(i).rsplit('_')
        if len(file_unix) < 2: continue
        if file_unix[1] not in ['full', 'incremental']: continue
        #
        if int(file_unix[2]) + EXPIRE_TIME < int(time.time()): 
            os.remove(i)
            logging.info("删除文件，" + i)
    except Exception as e: 
        logging.error("处理旧文件异常，" + str(e))


logging.info("备份完成.")
