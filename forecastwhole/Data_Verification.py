#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2020年7月1日
@author: yuejing
'''
import pandas as pd
from Common import DatabaseHandle
from Common import eml
from common import wechat
import time
import os

def ExcelData(path='//192.168.2.16/share2/临时全公开/jimmy/SGM预测/'+time.strftime("%Y%m",time.localtime())):
	'''Pandas获取EXCEL数据'''
	path=path
	#获取文件夹下所有文件
	FileList=os.listdir(path)
	for file in FileList:
		#特殊处理结果
		if 'PV总量分月预测' in file:
			#低版本
			forecast1_temp= pd.read_excel(path+'/'+file, sheet_name=0,index_col=None,usecols='B:M')
			forecast1=forecast1_temp.iloc[[3,21]]
			#中版本
			forecast2_temp= pd.read_excel(path+'/'+file, sheet_name=1,index_col=None,usecols='B:M')
			forecast2=forecast2_temp.iloc[[3,21]]
			#高版本
			forecast3_temp= pd.read_excel(path+'/'+file, sheet_name=2,index_col=None,usecols='B:M')
			forecast3=forecast3_temp.iloc[[3,21]]
			forecast=pd.concat([forecast1,forecast2,forecast3])
		else:
			pass
	return forecast


def verification():
	emltext='Dear all:\n\n测试结果如下：\n\n'
	try:
		excel=ExcelData().values.tolist()
		data=DatabaseHandle.Database()
		database=data.Pandas_Sql('./Sql/sql1.txt').values.tolist()
		month=data.Pandas_Sql('./Sql/sql2.txt').values.tolist()
		#数据对比
		Errornum=0
		for i in range(len(excel)):
			if excel[i][month[0][0]:]==database[i][month[0][0]:]:
				pass
			else:
				Errornum=Errornum+1
		#结果打印
		if Errornum==0:
			emltext=emltext+'整体预测数据，<b>验证通过</b>!'
		else:
			emltext=emltext+'整体预测数据，<b>验证不通过</b>！\n'
			emltext=emltext+'\n低版本-批发数:\n'+str(excel[0][month[0][0]:])+'\n'+str(database[0][month[0][0]:])+'\n低版本-上险数:\n'+str(excel[1][month[0][0]:])+'\n'+str(database[1][month[0][0]:])
			emltext=emltext+'\n中版本-批发数:\n'+str(excel[2][month[0][0]:])+'\n'+str(database[2][month[0][0]:])+'\n中版本-上险数:\n'+str(excel[3][month[0][0]:])+'\n'+str(database[3][month[0][0]:])
			emltext=emltext+'\n高版本-批发数:\n'+str(excel[4][month[0][0]:])+'\n'+str(database[4][month[0][0]:])+'\n高版本-上险数:\n'+str(excel[5][month[0][0]:])+'\n'+str(database[5][month[0][0]:])
	except Exception as e:
		emltext=emltext+'\n'+str(e)
		
	return emltext


if __name__ == "__main__":
	text=verification()
	wechat.send_weixin('SGM整体预测数据校验',text)
	print(text)
	eml.emlHandle().emilSend(['liweilong@way-s.cn','caixu@way-s.cn','yuejing@way-s.cn'],'整体预测数据校验',text)




