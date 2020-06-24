#!/usr/bin/python3
# -*- coding:utf-8 -*-
'''
Created on 2019年5月21日
@author: yuejing
'''
import xlrd
import xlwt
import yaml
import os 
from ruamel import yaml
from xlutils.copy import copy

class configyaml:
	def __init__(self,file_path):
		self.path=file_path

	def writeyaml(self,yaml_text):
		file = open(self.path, 'a', encoding='utf-8')
		yaml.dump(yaml_text, file, Dumper=yaml.RoundTripDumper)
		file.write('\n')
		file.close()

	def readyaml(self):
		abspath = os.path.dirname(os.path.abspath(__file__))
		directory=os.path.join(abspath, self.path)
		file = open(directory, 'r', encoding="utf-8")
		file_data = file.read()
		file.close()
		data = yaml.load(file_data,Loader=yaml.Loader)#yaml数据为字典或列表
		return data
