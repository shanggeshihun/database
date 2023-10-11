# _*_coding:utf-8 _*_
# @Time　　 : 2020/6/5   12:01
# @Author　 : zimo
# @File　   :get_conf.py
# @Software :PyCharm
# @Theme    :
import configparser
import os,sys
def get_es_conf():
    section='es'
    mysql_conf_dic={}
    path=os.path.join(sys.path[0],'config.properties')
    path = os.path.abspath(os.path.join(os.getcwd(),'..','config.properties'))
    conf=configparser.ConfigParser()
    conf.read(path)
    es_conf_dict={'host':conf.get(section,'host'),'port':conf.get(section,'port')}
    return es_conf_dict

def get_oracle_conf():
    section = 'oracle'
    oracle_conf_dic={}
    path=os.path.join(sys.path[0],'config.properties') # 打包路径会改变
    path = os.path.abspath(os.path.join(os.getcwd(), '..', 'config.properties'))
    conf=configparser.ConfigParser()
    conf.read(path)
    oracle_conf_dic={
        'host':conf.get(section,"host"),
        'port':conf.get(section,"port"),
        'user':conf.get(section,"user"),
        'password':conf.get(section,"password"),
        'instance':conf.get(section,"instance")
    }
    return oracle_conf_dic

def get_mysql_conf():
    section = 'mysql'
    mysql_conf_dic={}
    path=os.path.join(sys.path[0],'config.properties') # 打包路径会改变
    path = os.path.abspath(os.path.join(os.getcwd(), '..', 'config.properties'))
    conf=configparser.ConfigParser()
    conf.read(path)
    mysql_conf_dic={
        'host':conf.get(section,"host"),
        'port':conf.get(section,"port"),
        'user':conf.get(section,"user"),
        'password':conf.get(section,"password"),
        'database':conf.get(section,"database")
    }
    return mysql_conf_dic

def get_update_es_gap():
    section = 'update_es_gap'
    gap_conf_dic = {}
    path = os.path.join(sys.path[0], 'config.properties')  # 打包路径会改变
    path = os.path.abspath(os.path.join(os.getcwd(), '..', 'config.properties'))
    conf = configparser.ConfigParser()
    conf.read(path)
    gap_conf_dic = {
        'gap_sec': conf.get(section, "gap_sec")
    }
    return gap_conf_dic['gap_sec']

def get_for_updating_tb_info():
    section = 'for_updating_fileds'
    gap_conf_dic = {}
    path = os.path.join(sys.path[0], 'config.properties')  # 打包路径会改变
    path = os.path.abspath(os.path.join(os.getcwd(), '..', 'config.properties'))
    conf = configparser.ConfigParser()
    conf.read(path)
    fields_string_conf_dic = {
        'fields_string': conf.get(section, "fields_string"),
        'table_name': conf.get(section, "table_name"),
    }
    return fields_string_conf_dic