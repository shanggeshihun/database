# -*- coding: utf-8 -*-
"""
Created on 20190418
"""

"""
create table event_update_delete_insert
(
data_dt date,
Log_name varchar(100),
Event_start_time time,
Pos varchar(10),
Event_type varchar(30),
Server_id varchar(10),
End_log_pos varchar(10),
info varchar(255)
)

create table event_alter
(
data_dt date,
Log_name varchar(100),
Event_start_time time,
Pos varchar(10),
Event_type varchar(30),
Server_id varchar(10),
End_log_pos varchar(10),
info varchar(255)
)

"""



"""
fpath=r"D:\test1.txt"
#获取"at"所在的行号
def get_at_index(fpath):
    with open(fpath,'r',encoding='utf-8') as binlog:
        log=binlog.readlines()
        logLst=[line.strip() for line in log]
    #    以at开始的行号列表
        at_index=[i for i,line in enumerate(logLst) if line.startswith('# at') ]
        lastLine=len(logLst)
        at_index.append(lastLine)
    return logLst,at_index

logLst,at_index=get_at_index(fpath)

recordLst=[]
for i,index in enumerate(at_index[:-1]):
    start_index=at_index[i]
    end_index=at_index[i+1]
#        print(start_index,end_index)
    theEvent=logLst[start_index:end_index]
theEvent=logLst[158:171]
if 'Query' in  theEvent[0] or 'SET TIMESTAMP' in theEvent[2] or 'ALTER TABLE' in theEvent[3]:
    lst_2=re.findall(r"(\d+\s\d+:\d+:\d+)\sserver\sid\s(\d+)\s+end_log_pos\s+(\d+)\s+Query\s+thread_id=(\d+)\s+exec_time=(\d+)\s+error_code=(\d+)",theEvent[1])
    print(theEvent[1])
    
    Event_start_time='20'+lst_2[0][0]
    Pos=re.findall(r"(\d+)",theEvent[0])[0]
    Event_type=lst_2[0][3].split(" ")[0]
    Server_id=lst_2[0][1]
    End_log_pos=lst_2[0][2]
    
    sql=theEvent[3:]
    sql=[s.replace("#","").strip() for s in sql]
    info=" ".join(sql)
    record=(Log_name,Event_start_time,Pos,Event_type,Server_id,End_log_pos,info)
    print(record)

"""


import pymysql
import re
import datetime

Log_name=''


fpath=r"D:\test2.txt"
#获取"at"所在的行号
def get_at_index(fpath):
    """
    fpath:binlog所在路径
    return:binlog的line列表,at所在的行号
    """
    with open(fpath,'r',encoding='utf-8') as binlog:
        log=binlog.readlines()
        logLst=[line.strip() for line in log]
    #    以at开始的行号列表
        at_index=[i for i,line in enumerate(logLst) if line.startswith('# at') ]
    return logLst,at_index


#获取delete、update、write(insert)事件记录列表
def get_update_delete_insert(logLst,at_index):
    """
    logLst:指定binlog的line列表
    at_index:at所在的行号
    return:delete、update、write(insert)事件记录列表
    """
    recordLst=[]
    for i,index in enumerate(at_index[:-1]):
        start_index=at_index[i]
        end_index=at_index[i+1]
    #        print(start_index,end_index)
        theEvent=logLst[start_index:end_index]

        if 'Write_rows' in  theEvent[1] or 'Update_rows' in theEvent[1] or 'Delete_rows' in theEvent[1]:
            lst_2=re.findall(r"(\d+\s\d+:\d+:\d+)\sserver\sid\s(\d+)\s+end_log_pos\s+(\d+)\s+(.*)",theEvent[1])
            
            Event_start_time='20'+lst_2[0][0]
            Pos=re.findall(r"(\d+)",theEvent[0])[0]
            Event_type=lst_2[0][3].split(":")[0]
            Server_id=lst_2[0][1]
            End_log_pos=lst_2[0][2]
            
            sql=theEvent[2:]
            sql=[s.replace("#","").strip() for s in sql]
            info=" ".join(sql)
            record=(Log_name,Event_start_time,Pos,Event_type,Server_id,End_log_pos,info)
            recordLst.append(record)
    return recordLst


#获取delete、update、write(insert)事件记录列表
def get_alter(logLst,at_index):
    """
    logLst:指定binlog的line列表
    at_index:at所在的行号
    return:alter事件记录列表
    """
    recordLst=[]
    for i,index in enumerate(at_index[:-1]):
        start_index=at_index[i]
        end_index=at_index[i+1]
    #        print(start_index,end_index)
        theEvent=logLst[start_index:end_index]
        if 'Query' in  theEvent[1] and 'SET TIMESTAMP' in theEvent[2] and 'ALTER TABLE' in theEvent[3]:
            lst_2=re.findall(r"(\d+\s\d+:\d+:\d+)\sserver\sid\s(\d+)\s+end_log_pos\s+(\d+)\s+Query\s+thread_id=(\d+)\s+exec_time=(\d+)\s+error_code=(\d+)",theEvent[1])
            Event_start_time='20'+lst_2[0][0]
            Pos=re.findall(r"(\d+)",theEvent[0])[0]
            Event_type=lst_2[0][3].split(" ")[0]
            Server_id=lst_2[0][1]
            End_log_pos=lst_2[0][2]
            
            sql=theEvent[3:]
            sql=[s.replace("#","").strip() for s in sql]
            info=" ".join(sql)
            info=info.split(" /*!*/;")[0]
            record=(Log_name,Event_start_time,Pos,Event_type,Server_id,End_log_pos,info)
            recordLst.append(record)
    return recordLst


#日志写入相应表
def event_to_sql(etl_date,tbname,recordLst):
    conn=pymysql.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="052206",
            db="test0",
            charset="utf8"
            )
    cursor=conn.cursor()
    #清空表
    del_sql="delete from {0}".format(tbname)
    cursor.execute(del_sql)
    conn.commit()
    
    sql="insert into {0} values(%s,%s,%s,%s,%s,%s,%s,%s)".format(tbname)
    flag=True
    for i,r in enumerate(recordLst):
        record=(etl_date,)+r
        print("r",r)
        print("record",record)
        try:
            cursor.execute(sql,record)
            conn.commit()
        except Exception as e:
            print(e)
            flag=False
            break
    cursor.close()
    conn.close()
    if flag==True:
        print("insert into {0} Success".format(tbname))
    else:
        print("insert into {0} Failed".format(tbname))


if __name__=='__main__':
    fpath=r"D:\test1.txt"
    logLst,at_index=get_at_index(fpath)
    
    up_del_ins=get_update_delete_insert(logLst,at_index)
    
    up_del_ins_recordLst=get_update_delete_insert(logLst,at_index)
    alter_recordLst=get_alter(logLst,at_index)
    
    today=datetime.datetime.today()
    yesterday=today-datetime.timedelta(days=1)
    etl_date=datetime.date.strftime(yesterday,'%Y-%m-%d')
    
    event_to_sql(etl_date,"event_update_delete_insert",up_del_ins)
    event_to_sql(etl_date,"event_alter",alter_recordLst)


