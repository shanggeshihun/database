#数据库查询返回查询DataFrame结果
import pandas as pd
import pymysql
conn=pymysql.connect(
        host='192.168.0.204',
        port=6612,
        user='bi',
        passwd='79f6ba05a7e0bbb7dbcc4cc2fbdb15c9',
        db='repm',charset='utf8')
cursor=conn.cursor()
sql='select * from repm.rep_op_ord_p_line_info limit 2'
#执行语句
cursor.execute(sql)
#fetchall返回的元组
result=cursor.fetchall()
#列的信息
coldesc=cursor.description
#返回列名
colname=[]
[colname.append(coldesc[i][0]) for i in range(len(coldesc))]

df=pd.DataFrame(list(result))
conn.commit()
cursor.close()
conn.close()

df.columns=colname



