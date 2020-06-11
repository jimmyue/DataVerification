#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年6月11日
@author: yuejing
'''
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
import os
import time

#文件存储格式规范,每天生成一个文件夹，例如：//192.168.2.16/share2/临时全公开/lwl/20200611
def ExcelData(path='//192.168.2.16/share2/临时全公开/lwl/'+time.strftime("%Y%m%d",time.localtime())):
	'''Pandas获取EXCEL数据'''
	try:
		path=path
		#获取文件夹下所有文件
		FileList=os.listdir(path)
		for file in FileList:
			#别克smart数据
			if '地级市' in file:
				#index_col=None，不自动获取列名；usecols='A:K'，获取A:K列数据
				buick_temp= pd.read_excel(path+'/'+file, sheet_name=1,index_col=None,usecols='A:K')
				#重命名列
				buick_temp.columns=['年','月','日','省','地级市','品牌','车型','展厅流量','新增意向','新增订单','零售销量']
				#直销转换成别克
				buick_temp.loc[buick_temp['品牌']=='直销','品牌']='别克' 
				#筛选有效数据，str.strip()过滤前后空格
				buick=buick_temp[buick_temp['品牌'].str.strip()=='别克']
				#数据生成到list
				buick_list=[]
				buick_list.append(buick['展厅流量'].sum())
				buick_list.append(buick['新增意向'].sum())
				buick_list.append(buick['新增订单'].sum())
				buick_list.append(buick['零售销量'].sum())
			#雪佛兰smart数据
			elif '雪佛兰' in file:
				Chevrolet_temp=pd.read_excel(path+'/'+file, sheet_name=1,index_col=None,usecols='B:L')
				Chevrolet_temp.columns=['年','月','日','省','地级市','品牌','车系','展厅流量','新增意向','新增订单','零售销量']
				Chevrolet=Chevrolet_temp[Chevrolet_temp['品牌'].str.strip()=='雪佛兰']
				Chevrolet_list=[]
				Chevrolet_list.append(Chevrolet['展厅流量'].sum())
				Chevrolet_list.append(Chevrolet['新增意向'].sum())
				Chevrolet_list.append(Chevrolet['新增订单'].sum())
				Chevrolet_list.append(Chevrolet['零售销量'].sum())
			#凯迪拉克smart数据
			elif '凯迪拉克' in file:
				Cadillac_temp=pd.read_excel(path+'/'+file,sheet_name=0,index_col=None,usecols='A:B,D:M')
				Cadillac_temp.columns=['年','月','日','省','地级市','品牌','车系','展厅流量','新增意向','新增订单','零售销量','批发销量']
				Cadillac=Cadillac_temp[Cadillac_temp['品牌'].str.strip()=='凯迪拉克']
				Cadillac_list=[]
				Cadillac_list.append(Cadillac['展厅流量'].sum())
				Cadillac_list.append(Cadillac['新增意向'].sum())
				Cadillac_list.append(Cadillac['新增订单'].sum())
				Cadillac_list.append(Cadillac['零售销量'].sum())
				Cadillac_list.append(Cadillac['批发销量'].sum())
			#凯迪拉克APP smart数据
			elif '零售商' in file:
				CadillacApp_temp=pd.read_excel(path+'/'+file,sheet_name=0,index_col=None,usecols='A:K')
				CadillacApp_temp.columns=['年','月','日','省','地级市','品牌','车系','来电','来店','总订单','留存']
				CadillacApp=CadillacApp_temp[CadillacApp_temp['品牌'].str.strip()=='凯迪拉克']
				CadillacApp_list=[]
				CadillacApp_list.append(CadillacApp['来店'].sum())
				CadillacApp_list.append(CadillacApp['总订单'].sum())
			else:
				print('%s 非SMART数据文件！'% file)
		return buick_list,Chevrolet_list,Cadillac_list,CadillacApp_list
	
	except IOError:
		print("Error: 没有找到 %s 这个文件夹！" % path)

def read_txt(file_name):
	'''读取txt文件'''
	f = open(file_name, "r")
	str1 = f.read()
	f.close()
	return str1

def OracleData(db_link):
	'''Pandas数据库总量数据导出'''
	con=create_engine(db_link)
	#获取别克、雪佛兰、凯迪拉克数据
	sql1=read_txt('sql1.txt')
	data1=pd.read_sql(sql1,con)
	#获取凯迪拉克APP数据
	sql2=read_txt('sql2.txt')
	data2=pd.read_sql(sql2,con)
	return data1,data2

def CallProc(proc_name,db_link):
	'''调用存储过程'''
	con = cx_Oracle.connect(db_link)
	cur = con.cursor()
	#定义存储过程返回变量
	outVal = cur.var(str)
	cur.callproc(proc_name,[outVal])
	result=outVal.getvalue()
	con.close()
	return result

if __name__ == "__main__":
	#1.获取数据库数据
	a=OracleData('oracle://username:password@host:1521/server_name')
	#tolist(),将DataFrame转换成list
	a1=a[0].loc[1].tolist()[1:5]
	a2=a[0].loc[0].tolist()[1:5]
	a3=a[0].loc[2].tolist()[1:]
	a4=a[1].loc[0].tolist()[2:]
	aa=(a1,a2,a3,a4)
	#2.获取excel数据
	b=ExcelData()
	#3.调用存储过程
	c=CallProc('PROC_check_smart_test','username/password@host/server_name')

	#数据库与excel数据对比
	if aa==b:
		print('SMART数据总量验证通过！')
	else:
		print('SMART数据总量验证不通过！')
	#存储过程数据校验
	if '数据正常' in c:
		print('SMART存储过程验证通过!')
	else:
		print('SMART存储过程验证不通过!')

	#问题核查
	#print(aa,b,c)






