#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年6月11日
@author: yuejing
'''
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
from . import yamlHandle

class Database:
	Config = yamlHandle.configyaml('config.yaml').readyaml()
	def __init__(self,host=Config['database']['host'],user=Config['database']['username'],password=Config['database']['password'],db=Config['database']['server_name'],port=Config['database']['port']):
		self.host=host
		self.user=user
		self.password=password
		self.db=db
		self.port=port

	def Pandas_Sql(self,path):
		con=create_engine('oracle://'+self.user+':'+self.password+'@'+self.host+':'+str(self.port)+'/'+self.db)
		sql=read_txt(path)
		data=pd.read_sql(sql,con)
		return data

	def CallProc(self,proc_name):
		con = cx_Oracle.connect(self.user+'/'+self.password+'@'+self.host+':'+str(self.port)+'/'+self.db)
		cur = con.cursor()
		#定义存储过程返回变量
		outVal = cur.var(str)
		cur.callproc(proc_name,[outVal])
		result=outVal.getvalue()
		con.close()
		return result

def read_txt(file_name):
	'''读取txt文件'''
	f = open(file_name, "r")
	str1 = f.read()
	f.close()
	return str1