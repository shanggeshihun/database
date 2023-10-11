# _*_coding:utf-8 _*_
# @Time　　 : 2020/6/7   10:09
# @Author　 : zimo
# @File　   :run.py
# @Software :PyCharm
# @Theme    :更新楼栋所属的区划信息

from get_conf import get_oracle_conf,get_es_conf,get_mysql_conf,get_update_es_gap,get_for_updating_tb_info
from operator_oracle import  OracleConnect
from operator_mysql import MysqlConnect
from tuple_to_json import tuplst_to_jsonlst
from operator_es import OperatorEs
import time,sys


if __name__ == '__main__':
    # 待更新的数据信息
    for_updating_tb_info=get_for_updating_tb_info()
    fields_string = for_updating_tb_info['fields_string']
    table_name=for_updating_tb_info['table_name']
    sql = "select {0} from {1} where is_failed='1'".format(fields_string,table_name)
    print(sql)
    # 选择数据库
    print('选择楼栋更新映射表所在数据库：oracle：1，mysql：2。\n')
    choose_db_type=input('请输入数字:')

    start_time = time.time()

    if choose_db_type=='1':
        # 实例化oracle，查询待更新的数据信息
        oracle_conf = get_oracle_conf()
        username, password, host_ip, port, instance_name = oracle_conf['user'], oracle_conf['password'], oracle_conf[
            'host'], oracle_conf['port'], oracle_conf['instance']
        operator_oracle = OracleConnect(username, password, host_ip, port, instance_name)

        select_data = operator_oracle.get_data(sql)
    elif choose_db_type=='2':
        # 实例化mysql，查询待更新的数据信息
        mysql_conf = get_mysql_conf()
        username, password, host_ip, port, database = mysql_conf['user'], mysql_conf['password'], mysql_conf[
            'host'], mysql_conf['port'], mysql_conf['database']
        operator_mysql = MysqlConnect(username, password, host_ip, port, database)
        # sql = 'select lddm,ldmc,qudm,qumc,jddm,jdmc,sqdm,sqmc,wgdm,wgmc,lon,lat,tswg,tswgid,tswgmc from update_filed_mapping'

        select_data = operator_mysql.get_data(sql)
    else:
        print('不支持其他数据库,即将推出程序')
        sys.exit()


    # 数据库结果转换成bulk列表
    if not select_data:
        print('{0}暂无需要更新的数据，即将退出!'.format(table_name))
        sys.exit()
    tup_col=tuple(fields_string.split(','))
    filed_data_mapping=tuplst_to_jsonlst(tup_col,select_data)

    # 实例化es
    es_conf = get_es_conf()
    host, port = es_conf['host'], es_conf['port']
    operator_es = OperatorEs(host, port) # 实例化

    # 输入待更新索引名称
    print('\n提醒：适用于包含 {0} 字段的 索引\n'.format(fields_string))
    index_name=input('输入索引名称:') # 测试索引 ksj_test_update
    type_name=index_name

    i = 1
    error_lddm_list=[]
    lddm_list=[j['lddm'] for j in filed_data_mapping]
    for j in filed_data_mapping:
        # lddm,ldmc,qudm,qumc,jddm,jdmc,sqdm,sqmc,wgdm,wgmc,lon,lat,tswg,tswgid,tswgmc=j['lddm'],j['ldmc'],j['qudm'],j['qumc'],j['jddm'],j['jdmc'],j['sqdm'],j['sqmc'],j['wgdm'],j['wgmc'],j['lon'],j['lat'],j['tswg'],j['tswgid'],j['tswgmc']
        lddm=j['lddm']
        tmp = ["ctx._source['{0}']='{1}'".format(k, v) for k, v in j.items() if  v]
        script_string = ';'.join(tmp)
        body_data={
            "script":{
                "inline":script_string
            } ,
            "query": {
                "bool": {
                    "must": [
                        {
                            "term": {
                                "lddm":"{}".format(lddm)
                            }
                        }
                    ]
                }
            }
        }
        try:
            operator_es.update_data_bybody(index_name,body_data)
            print('第',i,'条lddm:',lddm,'更新成功')
        except Exception as error:
            print('第',i,'条lddm:',lddm,':更新失败',error)
            error_lddm_list.append(lddm)
            continue
        update_es_gap_sec=get_update_es_gap()
        time.sleep(int(update_es_gap_sec))
        i+=1
    end_time=time.time()
    print('更新结束，耗时:'+str(end_time-start_time)+'秒')

    # 回写更新ES的是否成功的状态
    error_lddm_set=set()
    if error_lddm_list:
        error_lddm_set=set(error_lddm_list)
        error_lddm=['\''+l+'\'' for l in error_lddm_set]
        error_lddm_str=','.join(error_lddm)
        flag_sql="update {0} set is_failed='1' where lddm in ({1})".format(table_name,error_lddm_str)
        if choose_db_type=='1':
            operator_oracle.update_date(flag_sql)
        else:
            operator_mysql.update_date(flag_sql)
        print('更新ES的楼栋状态已回写')

    success_lddm_set=set(lddm_list)-error_lddm_set
    success_lddm = ['\'' + l + '\'' for l in success_lddm_set]
    success_lddm_str = ','.join(success_lddm)
    flag_sql = "update {0} set is_failed='0' where lddm in ({1})".format(table_name,success_lddm_str)
    if choose_db_type == '1':
        operator_oracle.update_date(flag_sql)
    else:
        operator_mysql.update_date(flag_sql)
    print('更新ES的楼栋状态已回写')
    time.sleep(3)