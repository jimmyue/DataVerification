#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年6月11日
@author: yuejing
'''
import pandas as pd
from Common import DatabaseHandle
from Common import eml
import os

def ExcelData(path='./Data'):
	'''Pandas获取EXCEL数据'''
	try:
		path=path
		#获取文件夹下所有文件
		FileList=os.listdir(path)
		for file in FileList:
			#特殊处理结果
			if '特殊处理' in file:
				Special_temp= pd.read_excel(path+'/'+file, sheet_name=0,index_col=None,usecols='A:I')
				#重命名列
				Special_temp.columns=['ym','week','old_version_code','city_name','msrp','state_subsidy','local_subsidy','tp','flag']
				#按多列排序
				Special=Special_temp.sort_values(axis=0,ascending=True,by=['ym','week','old_version_code','city_name'])
				#空转值成-1
				Special['msrp']=Special['msrp'].fillna(-1)
				Special['state_subsidy']=Special['state_subsidy'].fillna(-1)
				Special['local_subsidy']=Special['local_subsidy'].fillna(-1)
				Special['tp']=Special['tp'].fillna(-1)
				#重新生成行索引
				Special.index = range(0,len(Special))
			elif '价格表' in file:
				#converters将字符串转换成浮点型  
				pricelist=pd.read_excel(path+'/'+file,converters={"2020年过渡期最终国家补贴金额": str,"官方补贴前指导价": str,"补贴后售价": str,"TP": str},sheet_name='15城市价格数据',skiprows=3,usecols=[0,47,58,62,66,67,63])
				pricelist.columns=['version_code','city_name','state_subsidy','msrp','tp_subsidized','tp','discount']
				#转值
				pricelist['msrp']=pricelist['msrp'].fillna('0')
				pricelist['state_subsidy']=pricelist['state_subsidy'].fillna('0')
				pricelist['tp']=pricelist['tp'].fillna('0')
				pricelist['discount']=pricelist['discount'].fillna('0')
				pricelist['tp_subsidized']=pricelist['tp_subsidized'].fillna('0')
				pricelist.loc[pricelist['msrp']=='待补齐参数后计算','msrp']='-2'
				pricelist.loc[pricelist['state_subsidy']=='待补齐参数后计算','state_subsidy']='-2'
				pricelist.loc[pricelist['tp_subsidized']=='待补齐参数后计算','tp_subsidized']='-2'
				pricelist.loc[pricelist['tp']=='待补齐参数后计算','tp']='-2'
				pricelist.loc[pricelist['discount']=='待补齐参数后计算','discount']='-2'
				pricelist.loc[pricelist['tp']=='无车','tp']='-1'
				pricelist.loc[pricelist['discount']=='无车','discount']='-1' 
				#转换数据类型
				pricelist[['state_subsidy','msrp','tp_subsidized','tp','discount']]=pricelist[['state_subsidy','msrp','tp_subsidized','tp','discount']].astype('float')

			else:
				print('%s 非NevPrice数据文件！'% file)
		return Special,pricelist
	
	except IOError:
		print("Error: 没有找到 %s 这个文件夹！" % path)

def verification(ymw):
	#获取excel数据
	result=ExcelData()
	#数据库
	data=DatabaseHandle.Database()
	#特殊处理结果
	data1=data.Pandas_Sql('./Sql/sql1.txt')
	#新能源价格结果，注意修改时间
	data2=data.Pandas_Sql('./Sql/sql2.txt',ymw)
	emltext='Dear all:\n\n测试结果如下：\n\n'
	#1.特殊处理验证
	SpecialData=pd.merge(result[0],data1,how='left',on=['ym','week','old_version_code','city_name'])
	SpecialData['msrp_y']=SpecialData['msrp_y'].fillna(-3)
	SpecialData['state_subsidy_y']=SpecialData['state_subsidy_y'].fillna(-3)
	SpecialData['local_subsidy_y']=SpecialData['local_subsidy_y'].fillna(-3)
	SpecialData['tp_y']=SpecialData['tp_y'].fillna(-3)
	SpecialData['flag_y']=SpecialData['flag_y'].fillna(-3)
	Errornum=0
	for index, row in SpecialData.iterrows():
		if round(row['msrp_x'])!=round(row['msrp_y']) or round(row['state_subsidy_x'])!=round(row['state_subsidy_y']) or round(row['local_subsidy_x'])!=round(row['local_subsidy_y']) or round(row['tp_x'])!=round(row['tp_y']) or row['flag_x']!=row['flag_y']:
			print('数据错误型号：'+row['old_version_code'])
			Errornum=Errornum+1
	if Errornum==0:
		emltext=emltext+'特殊处理数据正确!\n'
	else:
		emltext=emltext+'特殊处理数据错误!\n'
	#2.新能源价格验证
	PriceData=pd.merge(result[1],data2,how='left',on=['version_code','city_name'])
	PriceData['msrp_y']=PriceData['msrp_y'].fillna(-3)
	PriceData['state_subsidy_y']=PriceData['state_subsidy_y'].fillna(-3)
	PriceData['tp_y']=PriceData['tp_y'].fillna(-3)
	PriceData['discount_y']=PriceData['discount_y'].fillna(-3)
	PriceData['tp_subsidized_y']=PriceData['tp_subsidized_y'].fillna(-3)
	#数据对比
	version_error=PriceData.loc[PriceData['msrp_y']==-3,'version_code'].unique()
	msrp_error=PriceData.loc[(round(PriceData['msrp_x'])!=round(PriceData['msrp_y'])) & (PriceData['msrp_y']!=-3) & (PriceData['msrp_x']!=-2),'version_code'].unique()
	state_subsidy_error=PriceData.loc[(round(PriceData['state_subsidy_x'])!=round(PriceData['state_subsidy_y'])) & (PriceData['state_subsidy_y']!=-3) & (PriceData['state_subsidy_x']!=-2),'version_code'].unique()
	tp_error=PriceData.loc[(round(PriceData['tp_x'])!=round(PriceData['tp_y'])) & (PriceData['tp_y']!=-3) & (PriceData['tp_x']!=-2),'version_code'].unique()
	discount_error=PriceData.loc[(round(PriceData['discount_x'])!=round(PriceData['discount_y'])) & (PriceData['discount_y']!=-3) & (PriceData['discount_x']!=-2),'version_code'].unique()
	tp_subsidized_error=PriceData.loc[(round(PriceData['tp_subsidized_x'])!=round(PriceData['tp_subsidized_y'])) & (PriceData['tp_subsidized_y']!=-3) & (PriceData['tp_subsidized_x']!=-2),'version_code'].unique()
	emltext=emltext+'\n数据库未同步型号：'+str(version_error)+'\n指导价不相等型号：'+str(msrp_error)+'\n国家补贴不相等型号：'+str(state_subsidy_error)+'\n成交价不相等型号：'+str(tp_error)+'\n折扣不相等型号：'+str(discount_error)+'\n补贴后售价不相等型号：'+str(tp_subsidized_error)
	#3.执行存储过程
	ProcResult=data.CallProc('pkg_sgm_check.proc_nev_price')
	emltext=emltext+'\n\n存储过程执行结果：'+ProcResult
	return emltext


if __name__ == "__main__":
	text=verification('2020066')
	print(text)
	#eml.emlHandle().emilSend('yuejing@way-s.cn','Smart数据校验',text)




