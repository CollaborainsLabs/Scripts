import pandas as pd
import numpy as np
import requests,os,zipfile,shutil
from datetime import date,datetime,timedelta
import numpy as np
import math
import time

def ingest_data():

#Part1- Data Collection
	start_date = datetime.now()
	end_date = datetime(2001,12,31)
	diff= timedelta(days=1)
	while(start_date>=end_date):
		start_date = start_date - diff
		url = "https://www1.nseindia.com/archives/fo/bhav/fo{day}.zip".format(day = start_date.strftime("%d%m%y"))
	
		r = requests.get(url)
		if (r.status_code != 200):
			print((r.status_code,start_date.strftime("%d-%m-%y")))

		else:
			print('success')
			fname = './data/cmprssd/' + start_date.strftime("%d-%m-%y") +'.zip'
			with open(fname,'wb') as f:
				f.write(r.content)

#Part2- Zip file extraction
	for file in os.listdir("./data/cmprssd/"):
		if file.endswith(".zip"):
			file = './data/cmprssd/' + file
			try:
				zip = zipfile.ZipFile(file)
				zip.extractall(path = './data/rqrd/')
			except Exception as e:
				print("an error occured with file",file)
				source = './data/cmprssd/' + str(file)
				destination = './data/error/' + str(file)
				shutil.move(source,destination)	
			else:
				print("success")
#Part3- Aggregation
	df = pd.DataFrame()
	data_path = './data/rqrd/'
	for data_file in os.listdir(data_path):
		if data_file.endswith(".csv"):
			data_file_path = data_path + data_file
			df_temp = pd.read_csv(data_file_path)
			df= df.append(df_temp)
	df.to_csv('./output/vol_data.csv',index=False) #appending to csv


ingest_data() #Function call