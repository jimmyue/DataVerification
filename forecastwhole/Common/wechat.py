import requests

def send_weixin(title=None,text=None):
	if title==None and text==None:
		print('请输入标题和正文！')
	elif title!=None and text!=None:
		url='http://sc.ftqq.com/SCU104645Tb11d8bb3e658dd30232e11b8252609a85f03d9801a145.send?text='+title+'&desp='+text
	else:
		url='http://sc.ftqq.com/SCU104645Tb11d8bb3e658dd30232e11b8252609a85f03d9801a145.send?text='+title

	r=requests.get(url)