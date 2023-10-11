# _*_coding:utf-8 _*_
# @Time　　 : 2020/6/5   18:14
# @Author　 : zimo
# @File　   :operator_oracle.py
# @Software :PyCharm
# @Theme    :cursor迭代，且，针对目标es类型是nested

import cx_Oracle,time
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
class OracleConnect():
    def __init__(self,username,password,host_ip,port,instance_name):
        '''
        :param username:    数据库用户名
        :param password:    数据库密码
        :param host_ip:     数据库主机IP地址
        :param port:        数据库主机端口
        :param instance_name:   数据库实例名称
        :param sql:         数据库执行SQL语句
        '''
        self.username = username
        self.password = password
        self.host_ip = host_ip
        self.port = port
        self.instance_name = instance_name

    def get_data(self,sql):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username,self.password,self.host_ip,self.port,self.instance_name) )#连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute (sql) #执行SQL语句
            data = cursor.fetchall()  #取出数据库所有数据，该操作慢
            cursor.close()
            conn.close()
            return data     #返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def get_dic(self,sql):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username,self.password,self.host_ip,self.port,self.instance_name) )#连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result=cursor.execute (sql) #执行SQL语句
            index = cursor.description
            cursor.close()
            conn.close()
            return index     #返回数据
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def update_date(self,sql):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username,self.password,self.host_ip,self.port,self.instance_name) )#连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result=cursor.execute (sql) #执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def insert_date(self,sql):
        try:
            conn = cx_Oracle.connect('{}/{}@{}:{}/{}'.format(self.username,self.password,self.host_ip,self.port,self.instance_name) )#连接数据库字符串
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute (sql) #执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))