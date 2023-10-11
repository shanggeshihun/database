# -*- coding: utf-8 -*-


import json
import pymysql

def data_insert():
    #json格式数据
    data={
       "camera": {
          "created": "1531925035",
          "type": 1,
          "description": "A bridge is a structure that is built over a railway, river, or road so that people or vehicles can cross from one side to the other.",
          "location": {
             "locationID":"1",
             "country": "china",
             "city": "xian",
             "region": "changanqu"
          },
          "project_id": "Golden Gate Bridge",
          "task_id": "23232",
          "camera_id": "23232",
          "mu": "mm",
          "value": {
             "x": "123123",
             "y": "123123"
          },
          "ext_info": "null"
       }
    }

#    服务器，用户名，密码，数据库
    db=pymysql.connect("localhost","root","052206","ceshi")
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor=db.cursor()
    
    # 使用 execute() 方法执行 SQL，如果表存在则删除
    cursor.execute("drop table if exists camera")
    
    # 使用预处理语句创建表
    sql="create table camera (creat_time varchar(255),country varchar(255))"
    
    cursor.execute(sql)
    print(type(data))
    
    print(data)
    
    insert_to="insert into camera (creat_time,country) values (%s,%s)"
    print(insert_to)
    
    insert_val=(data["camera"]["created"],data["camera"]["location"]["country"])
    print(insert_val)
    
    try:
        # 执行sql语句
        cursor.execute(insert_to,insert_val)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()
        
    cursor.close()
    
if __name__=="__main__":
    data_insert()


