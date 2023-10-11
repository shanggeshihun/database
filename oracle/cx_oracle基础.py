# coding:utf-8
import cx_Oracle
# 连接数据库
# conn = cx_Oracle.connect('用户名/密码@IP:端口号/SERVICE_NAME')
conn=cx_Oracle.connect('system/123456@localhost:1521/orcl')
# 使用cursor()方法获取操作游标
cursor=conn.cursor()
# 使用execute方法执行sql语句
cursor.execute('select * from emp')

# 获取所有数据-元组的组成列表
all_data=cursor.fetchall()
print('获取所有数据:\n',all_data)

# 获取部分数据
many_data=cursor.fetchmany(4)
print('获取部分数据:\n',many_data)

# 一次返回一行
print('一次返回一行\n')
while 1:
	rs=cursor.fetchone()
	if not rs:
		break
	print(rs)
		
cursor.close()
conn.close()

conn=cx_Oracle.connect('system/123456@localhost:1521/orcl')
cursor=conn.cursor()
cursor.execute('select * from emp')
# 重新连接，一次返回一行
print('重新连接，一次返回一行\n')
while 1:
	rs=cursor.fetchone()
	if not rs:
		break
	print(rs)