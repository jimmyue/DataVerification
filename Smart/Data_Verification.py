#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年6月11日
@author: yuejing
'''
import pandas as pd
from common import DatabaseHandle
from common import eml
import os
import datetime
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
				#定义全局变量，获取excel最小时间，数据库根据execl时间查询
				global mintime
				mintime=CadillacApp['日'].unique().min().replace('/','')
				CadillacApp_list=[]
				CadillacApp_list.append(CadillacApp['来店'].sum())
				CadillacApp_list.append(CadillacApp['总订单'].sum())
			else:
				print('%s 非SMART数据文件！'% file)
		return buick_list,Chevrolet_list,Cadillac_list,CadillacApp_list
	
	except IOError:
		print("Error: 没有找到 %s 这个文件夹！" % path)


def Verification():
	'''数据校对'''
	#1.获取excel数据
	b=ExcelData()

	#2.获取数据库数据
	data=DatabaseHandle.Database()
	data1=data.Pandas_Sql('./sql/sql1.txt')
	data2=data.Pandas_Sql('./sql/sql2.txt',mintime)
	#tolist(),将DataFrame转换成list
	a1=data1.loc[1].tolist()[1:5]
	a2=data1.loc[0].tolist()[1:5]
	a3=data1.loc[2].tolist()[1:]
	a4=data2.loc[0].tolist()[1:]
	a=(a1,a2,a3,a4)

	#3.调用存储过程
	c=data.CallProc('PROC_check_smart_test')

	#4.数据校对
	#数据库与excel数据对比
	emltext='Dear all:\n\n测试结果如下：\n\n'
	if a==b:
		emltext=emltext+'SMART数据总量<b>验证通过</b>！\n'
	else:
		emltext=emltext+'SMART数据总量<b>验证不通过</b>！\n'+'excel数据: '+str(b)+'\n数据库数据: '+str(a)+'\n\n'
	#存储过程数据校验
	if '数据正常' in c:
		emltext=emltext+'SMART存储过程<b>验证通过</b>!\n'+'存储过程执行结果：'+c
	else:
		emltext=emltext+'SMART存储过程<b>验证不通过</b>!\n'+'存储过程执行结果：'+c
	return emltext


if __name__ == "__main__":
	text=Verification()
	print(text)
	#eml.emlHandle().emilSend('yuejing@way-s.cn','Smart数据校验',text)









