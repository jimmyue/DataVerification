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
		if '细分市场预测' in file:
			#中版本
			forecast1= pd.read_excel(path+'/'+file,skiprows=1,sheet_name=3,usecols='B:N')
			forecast1.columns=['segment','JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE','JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER']
			forecast1=forecast1.drop_duplicates()
			#MD版本
			forecast2= pd.read_excel(path+'/'+file,sheet_name=0,usecols=[1,21])
			forecast2.columns=['segment','sales']
			forecast2=forecast2.loc[(forecast2['segment'] == 'Medium High SUV') | (forecast2['segment'] =='Medium Low SUV')]
		else:
			pass
	return forecast1,forecast2


def verification():
	emltext='Dear all:\n\n测试结果如下：\n\n'
	try:
		text=ExcelData()
		data=DatabaseHandle.Database()
		database=data.Pandas_Sql('./Sql/sql1.txt')
		MediumSuv=data.Pandas_Sql('./Sql/sql2.txt').values.tolist()
		month=data.Pandas_Sql('./Sql/sql3.txt').values.tolist()[0][0]
		#合并DataFrame
		MergeData=pd.merge(text[0],database,how='left',on=['segment'])
		MergeData=MergeData.dropna(subset=['december'])
		MergeData=MergeData.round({'JANUARY':0,'FEBRUARY':0,'MARCH':0,'APRIL':0,'MAY':0,'JUNE':0,'JULY':0,'AUGUST':0,'SEPTEMBER':0,'OCTOBER':0,'NOVEMBER':0,'DECEMBER':0})
		#细分市场预测数据对比
		Errornum=0
		for index, row in MergeData.iterrows():
			if row['JANUARY']!=row['january']:
				Errornum=Errornum+1
				emltext=emltext+'一月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['JANUARY']),str(row['january']))
			elif row['FEBRUARY']!=row['february']:
				Errornum=Errornum+1
				emltext=emltext+'二月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['FEBRUARY']),str(row['february']))
			elif row['MARCH']!=row['march']:
				Errornum=Errornum+1
				emltext=emltext+'三月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['MARCH']),str(row['march']))
			elif row['APRIL']!=row['april']:
				Errornum=Errornum+1
				emltext=emltext+'四月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['APRIL']),str(row['april']))
			elif row['MAY']!=row['may']:
				Errornum=Errornum+1
				emltext=emltext+'五月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['MAY']),str(row['may']))
			elif row['JUNE']!=row['june']:
				Errornum=Errornum+1
				emltext=emltext+'六月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['JUNE']),str(row['june']))
			elif row['JULY']!=row['july']:
				Errornum=Errornum+1
				emltext=emltext+'七月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['JULY']),str(row['july']))
			elif row['AUGUST']!=row['august']:
				Errornum=Errornum+1
				emltext=emltext+'八月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['AUGUST']),str(row['august']))
			elif row['SEPTEMBER']!=row['september']:
				Errornum=Errornum+1
				emltext=emltext+'九月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['SEPTEMBER']),str(row['september']))
			elif row['OCTOBER']!=row['october']:
				Errornum=Errornum+1
				emltext=emltext+'十月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['OCTOBER']),str(row['october']))
			elif row['NOVEMBER']!=row['november']:
				Errornum=Errornum+1
				emltext=emltext+'十一月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['NOVEMBER']),str(row['november']))
			elif row['DECEMBER']!=row['december']:
				Errornum=Errornum+1
				emltext=emltext+'十二月预测数据不一致:%s[EXCEL:%s 数据库:%s]\n' % (str(row['segment']),str(row['DECEMBER']),str(row['december']))
		if Errornum==0:
			emltext=emltext+'非拆分细分市场预测数据<b>验证通过</b>！'
		#MediumSuv拆分
		if len(text[1])==2:
			ProcResult=data.CallProc('proc_check_forecast_segment2')
			ratiosale=text[1].values.tolist()
			allsale=text[0].loc[text[0]['segment'] == 'Medium SUV'].values.tolist()
			resultlist=[['Medium High SUV',0,0,0,0,0,0,0,0,0,0,0,0],['Medium Low SUV',0,0,0,0,0,0,0,0,0,0,0,0]]
			for i in range(1,13):
				resultlist[0][i]=round(allsale[0][i]*ratiosale[0][1]/(ratiosale[0][1]+ratiosale[1][1]))
				resultlist[1][i]=round(allsale[0][i]*ratiosale[1][1]/(ratiosale[0][1]+ratiosale[1][1]))
            #拆分校对
			Errornum=0
			for i in range(month+1,13):
				if MediumSuv[0][i]==resultlist[0][i] and MediumSuv[1][i]==resultlist[1][i]:
					pass
				else:
					Errornum=Errornum+1
			#MediumSuv拆分结果打印
			if Errornum==0:
				emltext=emltext+'\n\nMedium Suv拆分预测数据<b>验证通过</b>！\n'
			else:
				emltext=emltext+'\n\nMedium Suv拆分预测数据<b>验证不通过</b>！\n'
				emltext=emltext+str(resultlist)+'\n'
				emltext=emltext+str(MediumSuv)+'\n'
		else:
			ProcResult=data.CallProc('proc_check_forecast_segment1')
		#存储过程结果打印
		if '正常' in str(ProcResult):
			emltext=emltext+'\n存储过程<b>验证通过</b>！\n'
		else:
			emltext=emltext+'\n存储过程<b>验证不通过</b>！\n'
			emltext=emltext+'存储过程执行结果：'+str(ProcResult)

	except Exception as e:
		emltext=emltext+str(e)

	return emltext


if __name__ == "__main__":
	text=verification()
	wechat.send_weixin('SGM细分市场预测数据校验',text)
	print(text)
	eml.emlHandle().emilSend(['liweilong@way-s.cn','caixu@way-s.cn','yuejing@way-s.cn'],'细分市场预测数据校验',text)


