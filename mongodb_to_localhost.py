#coding:utf-8
"""
201490328 mongodb 数据库写入本地localhost数据库
"""
import pymongo
#import time
import pandas as pd


myclient=pymongo.MongoClient("mongodb://192.168.0.214:27017/")
mydb=myclient['ropservice_drivervehiclecertify1']
# 查看一天记录数据
mycol=mydb['driver_vehicle_certify_his']
# 所有记录数量
docu_cnt=mycol.count()
myresult1=mycol.find({"drivercertification":{"usercode":"100000118871"}})
myresult2=mycol.find({"usercode":'100000120001'})
for each in myresult2:
    print(each)

parent_first_document=list(mycol.find().limit(4))[0]
for each in parent_first_document:
    print(each)


#数据探索
#vehicle以字典k长度不一致
temp=mycol.find({'hisid':'adef96e2202e4eab89972ba3e2153476'})
for each in temp:
    print(each['drivercertification'])
temp=mycol.find({'hisid':'c5690d5b17994801884231b9bd465f59'})
for each in temp:
    print(each['drivercertification'])
parent_first_document=list(mycol.find().limit(4))[0]
#无键drivercertification
temp=mycol.find({'hisid':'72d12f7ca2474f81b29e452373144111'})
for each in temp:
    print(each)

#main数据
main_k_lst=[]
for d in mycol.find():
    for i in range(len(d)):
        i_k=list(d.keys())
        for m in i_k:
            if m not in main_k_lst:
                main_k_lst.append(m)
main_v_lst=[]
for d in mycol.find():
    #初始化完整字段的字典
    sample_dic={k:'' for k in main_k_lst}
    lst=[]
    #第i个字典的键值
    i_k=[k for k in d.keys()]
    for s_k in sample_dic:
        if s_k in i_k:
            sample_dic[s_k]=d[s_k]
        else:
            sample_dic[s_k]=''
    main_v_lst.append(sample_dic.values())
main_v_culumns=main_k_lst.copy()
main_df=pd.DataFrame(main_v_lst)
main_df.columns=main_v_culumns

#dri数据
dri_k_lst=[]
for d in mycol.find():
    #dri_sub 单个字典
    if 'drivercertification' in d.keys():
        dri_sub=d['drivercertification']
        i_k=list(dri_sub.keys())
        for m in i_k:
            if m not in dri_k_lst:
                dri_k_lst.append(m)
dri_k_lst.insert(0,'_id')
dri_k_lst.insert(1,'hisid')
dri_k_lst.insert(2,'changefrom')
    #dri所有values
dri_v_lst=[]
for d in mycol.find():
#    veh_sub 字典为元素的列表
    if 'drivercertification' in d.keys():
        dri_sub=d['drivercertification']
        #初始化完整字段的字典
        sample_dic={k:'' for k in dri_k_lst}
        lst=[]
        #第i个字典的键值
        i_k=[k for k in dri_sub.keys()]
        for s_k in sample_dic:
            if s_k in i_k:
                sample_dic[s_k]=dri_sub[s_k]
            else:
                sample_dic[s_k]=''
        sample_dic['_id']=d['_id']
        sample_dic['hisid']=d['hisid']
        sample_dic['changefrom']=d['changefrom']
        dri_v_lst.append(sample_dic.values())
dri_v_culumns=dri_k_lst.copy()
dri_df=pd.DataFrame(dri_v_lst)
dri_df.columns=dri_v_culumns

#veh数据(mongodb 大小写敏感，MySQL大小写不敏感)
veh_k_lst=[]
for d in mycol.find():
    #    veh_sub 字典为元素的列表
    veh_sub=d['vehicleinformation']
    for i in range(len(veh_sub)):
        i_k=list(veh_sub[i].keys())
        for m in i_k:
            if m not in veh_k_lst:
                veh_k_lst.append(m)
veh_k_lst.insert(0,'_id')
veh_v_lst=[]
for d in mycol.find():
#    veh_sub 字典为元素的列表
    veh_sub=d['vehicleinformation']
    for i in range(len(veh_sub)):
        #初始化完整字段的字典
        sample_dic={k:'' for k in veh_k_lst}
        lst=[]
        #第i个字典的键值
        i_k=[k for k in veh_sub[i].keys()]
        for s_k in sample_dic:
            if s_k in i_k:
                sample_dic[s_k]=veh_sub[i][s_k]
            else:
                sample_dic[s_k]=''
        sample_dic['_id']=d['_id']
        veh_v_lst.append(sample_dic.values())
veh_v_culumns=veh_k_lst.copy()
veh_df=pd.DataFrame(veh_v_lst)
veh_df.columns=veh_v_culumns
    #同一字段映射成字典
veh_df_copy=veh_df.copy()
    #同一字段映射成字典
merge_col_dic={}
    #后出现的同一字段改名映射成字典
new_name_dic={}
for i in range(len(veh_df_copy.columns)):
    for j in range(i,len(veh_df_copy.columns)):
        if veh_df_copy.columns[i]!=veh_df_copy.columns[j] and str.lower(veh_df_copy.columns[i])==str.lower(veh_df_copy.columns[j]):
            merge_col_dic[veh_df_copy.columns[i]]=veh_df_copy.columns[j]

            new_name='new_'+str(veh_df_copy.columns[j])
            new_name_dic[veh_df_copy.columns[j]]=new_name
veh_df_copy.columns
veh_df_copy.rename(columns=new_name_dic, inplace = True)

myclient.close()



#写入指定数据库函数
def write_to_mysql(DataFrame,mysql_table_name):
    import numpy as np
    import pandas as pd
    h='127.0.0.1'
    p=3306
    u='root'
    pw='052206'
    d='test0'
    from sqlalchemy import create_engine
    yconnect= create_engine('mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(u,pw,h,p,d))
    #每次写入50000条数据
    part_num=int(len(DataFrame)/50000)+1
    for i in range(part_num):
        start=i*50000
        end=(i+1)*50000
        DataFrame_1=DataFrame.iloc[start:end,:]
        DataFrame_1=DataFrame_1.applymap(lambda x:np.str(x))
        #分块写入MySQL
        pd.io.sql.to_sql(DataFrame_1,mysql_table_name, yconnect, schema=d,index=False, if_exists='append',chunksize=1000)
        print(i)

#写入指定数据库
write_to_mysql(dri_df,'rep_op_dri_mongodb_driver_certify_his')
dri_df.columns
write_to_mysql(main_df,'rep_op_dri_mongodb_driver_vehicle_certify_his')
main_df.columns
write_to_mysql(veh_df_copy,'rep_op_dri_mongodb_vehicle_certify_his')
veh_df_copy.columns
