import boto3
import os
import botocore
import csv
import numpy
import threading
import pandas as pd

##collects list of files with data 3.7
__Bucket_name = 'agco-fuse-production-lake'
__s3 = boto3.resource('s3')
count = 0
bucket = __s3.Bucket(__Bucket_name)
highPath  = ['json','csv']
setFiles = set()
cwd = os.getcwd()
print(cwd)
with open('DeletionFile.txt','r') as dF :
	listOfFiles = dF.readlines()
dF.close()
listOfDeletionFiles =[i.replace("\n","") for i in listOfFiles ]
for i in listOfDeletionFiles :
	if os.path.isfile(os.path.join(cwd,i)):
		print(i)
		os.remove(os.path.join(cwd,i))

for  path in highPath :
	os.system('aws s3 ls s3://agco-fuse-production-lake/'+path+'/ >'+path+".txt")
csv = open(highPath[0]+".txt",'r')
json = open(highPath[1]+".txt",'r')
csvLines = csv.readlines()
jsonLines = json.readlines()
listT= [{'csv':csvLines},{'json':jsonLines}]
#print(csvLines)
for obj in bucket.objects.all():
	if obj.size != 0 : 
		print("File Location: "+obj.key)
		print("File Size: "+str(obj.size))
		for filesL in listT :
			for key,filesLL in filesL.items() :
				for files in filesLL :
					striii = key+"/"+files.lstrip().split(" ")[1].replace("\n","").replace("/","")
					strii = files.lstrip().split(" ")[1].replace("\n","").replace("/","")
					print(key+" : "+striii)
					fileCreator = (key+strii)+".csv"
					if striii not in obj.key :
						fileCreator = (key+strii)+".csv"
						setFiles.add(fileCreator)
						print (fileCreator)
					else : 
						with open(fileCreator,"a") as fileWrite:
							fileWrite.write(obj.key+"\n")
												

with open("DeletionFile.txt","w") as writeFile :
	for i in setFiles :
		writeFile.write(i+"\n")
writeFile.close()
fileWrite.close()	
csv.close()
json.close()
os.remove('json.txt')
os.remove('csv.txt')