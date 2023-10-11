# _*_coding:utf-8 _*_
# @Time　　 : 2020/6/12   10:04
# @Author　 : zimo
# @File　   :OperateMysql.py
# @Software :PyCharm
# @Theme    :

import pymysql,time
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
class MysqlConnect():
    def __init__(self,username,password,host_ip,port,database):
        '''
        :param username:    数据库用户名
        :param password:    数据库密码
        :param host_ip:     数据库主机IP地址
        :param port:        数据库主机端口
        :param database:   数据库名称
        :param sql:         数据库执行SQL语句
        '''
        self.username = username
        self.password = password
        self.host_ip = host_ip
        self.port = port
        self.database = database

    def get_data(self,sql):
        try:
            conn = pymysql.connect(
                host=self.host_ip,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.database
            )
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute (sql) #执行SQL语句

            data = cursor.fetchall()  #取出数据库所有数据，该操作慢
            cursor.close()
            conn.close()
            return data     #返回数据
        except Exception as e:
            print(e)
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def get_dic(self,sql):
        try:
            conn = pymysql.connect(
                host=self.host_ip,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.database
            )
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
            conn = pymysql.connect(
                host=self.host_ip,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.database
            )
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            result=cursor.execute (sql) #执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

    def insert_date(self,sql):
        try:
            conn = pymysql.connect(
                host=self.host_ip,
                port=int(self.port),
                user=self.username,
                passwd=self.password,
                db=self.database
            )
            cursor = conn.cursor()  # 使用cursor()方法获取操作游标
            cursor.execute (sql) #执行SQL语句
            cursor.execute("commit")
            cursor.close()
            conn.close()
        except:
            print('\033[1;35m 执行失败：{}数据库连接失败！！！ \033[0m!'.format(self.host_ip))

if __name__ == '__main__':
    # 实例化mysql，查询待更新的数据信息
    from get_conf import get_mysql_conf, get_es_conf
    mysql_conf = get_mysql_conf()
    print(mysql_conf)
    username, password, host_ip, port, database = mysql_conf['user'], mysql_conf['password'], mysql_conf[
        'host'], mysql_conf['port'], mysql_conf['database']
    operator_mysql = MysqlConnect(username, password, host_ip, port, database)
    sql = 'select lddm,ldmc,qudm,qumc,jddm,jdmc,sqdm,sqmc,wgdm,wgmc,lon,lat,tswg,tswgid,tswgmc from update_filed_mapping'
    select_data = operator_mysql.get_data(sql)
    print(select_data)