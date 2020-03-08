

#!C:\Program Files\Python37

import pysftp
import boto3
import boto.s3.connection 
import datetime
import os 
import pyodbc
import zipfile
import time
import paramiko
from datetime import datetime
import sys
from subprocess import run
import hashlib
import math    

def sqlconnection():      
    try:          
        conn = pyodbc.connect("Driver={SQL Server Native Client 11.0};Server=hscsqldtci01t;Database=dtcidigital_metastore;Uid=_oozie;Pwd=seQaf4A6;ColumnEncryption=Enabled;")
        cur = conn.cursor()
        return cur
    except Exception as e:
        print(e)


def downloadfroms3tolocalandzipctrl():
    try:
        TOTAL_FILE_NAME_WITH_ZIP=''
        CONTROL_FILE_NAME=''          
        WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES="\\hscsqldtci01t\Work\dtcidigital\manifests".replace('\\','\\\\')
        S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES=WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES+'\\WFI_MANIFEST_'+str(57675)+'\\Workflow\\TMP\\'
        s3conn=boto3.resource('s3',aws_access_key_id='AKIAIGXHINAUWNKD222Q',aws_secret_access_key='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',region_name='us-east-1')
        my_bucket = s3conn.Bucket('quaero-snowflake-dis')
        keys=[] 
        tmp = "s3://quaero-snowflake-dis/TEST/DIS_MASTERING/FRM/DB_SEGMENT_EXTRACT_BK/WFI_57538/disney_25067_2020228.txt.gz".split("/")[:-1]          
        prefilter = "/".join(tmp[3:])          
        prefilter = prefilter + "/" 

        for object_summary in my_bucket.objects.filter(Prefix=prefilter):              
            filedirpath=object_summary              
            filenamelist=str(filedirpath).split(',')[-1:]              
            filelocins3=str(filenamelist).split('\'')[1]              
            keys.append(filelocins3)          
        #del keys[0]
        print(keys)
        print(tmp)
        print("Looping the files")

        no_of_files=len(keys)
        print("No of files ="+str(no_of_files))

        for key in keys:              
            localfullpath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])
            print(key)
            print(localfullpath)
            print(my_bucket)
            time.sleep(10)              
            my_bucket.download_file(key,localfullpath)              
            cur=sqlconnection()              
            cur.execute("SELECT CONTROL_FILE_FLG,CONTROL_FILE_EXT,CONTROL_FILE_MASK,SOURCE_FILE_MASK FROM M_SOURCE_ENTITY WHERE DATASET_ID= {0}".format(1000000594))              
            rows=cur.fetchall()              
            for row in rows:                  
                CONTROL_FILE_FLG=row.CONTROL_FILE_FLG                  
                CONTROL_FILE_EXT=row.CONTROL_FILE_EXT                  
                CONTROL_FILE_MASK=row.CONTROL_FILE_MASK                  
                SRC_FILE_MASK=row.SOURCE_FILE_MASK              
            FILE_MASK=SRC_FILE_MASK.replace('%','')              
            if(0==1):
                print("Zip file flag is 1")                  
                if(1==1):
                    print("Date Datetime flag is 1")                      
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDDHHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):
                            print(" Output File format is .txt.gz")                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD_HHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[:8]+'_' +str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[8:]                         
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                                               
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD"):                          
                        currentDT = datetime.today().strftime("%Y-%m-%d")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="MMDDYYYY"):                          
                        currentDT = datetime.today().strftime("%m-%d-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="DDMMYYYY"):                          
                        currentDT = datetime.today().strftime("%d-%m-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                             
                    TOTAL_FILE_NAME_WITH_ZIP=FILE_MASK+'_'+TIMESTAMP+'.zip'                  
                else:                      
                    TOTAL_FILE_NAME_WITH_ZIP=FILE_MASK+'.zip'                  
                zipfullpath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,TOTAL_FILE_NAME_WITH_ZIP)                  
                zip_file = zipfile.ZipFile(zipfullpath, 'w')                  
                os.chdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                
                zip_file.write(newfilename, compress_type=zipfile.ZIP_DEFLATED)              
            else:                  
                if(1==1):                      
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDDHHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD_HHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[:8]+'_' +str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[8:]                         
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):
                            print(" Output File format is .txt.gz")
                            print(TIMESTAMP)    
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'
                            print("New File Name is :"+newfilename)
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                       
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD"):                          
                        currentDT = datetime.today().strftime("%Y-%m-%d")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="MMDDYYYY"):                          
                        currentDT = datetime.today().strftime("%m-%d-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="DDMMYYYY"):                          
                        currentDT = datetime.today().strftime("%d-%m-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                else:
                    filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                    if(".txt.gz"==".csv"):                              
                        newfilename=FILE_MASK+'.csv'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)
                    if(".txt.gz"==".txt.gz"):                              
                        newfilename=FILE_MASK+'.txt.gz'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)                           
                    if(".txt.gz"==".txt"):                              
                        newfilename=FILE_MASK+'.txt'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)            
            time.sleep(10)              
            if(1==1):                  
                CONTROL_FILE_NAME=CONTROL_FILE_MASK.split('%')[0]+'_'+TIMESTAMP+CONTROL_FILE_EXT                  
                CONTROL_FILE_FULL_PATH=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,CONTROL_FILE_NAME)                  
                f=open(CONTROL_FILE_FULL_PATH, 'w')
                f.write("FILE="+newfilename)
                size_in_Bytes=file_size_in_Bytes(totnewfilepath)
                print("SIZE in KB="+str(size_in_Bytes))
                f.write("\nSIZE="+str(size_in_Bytes))
                with open(totnewfilepath,'rb') as f1:
                    bytes=f1.read()
                    readable_hash=hashlib.md5(bytes).hexdigest()
                print("MD5SUM="+str(readable_hash))
                f.write("\nMD5SUM="+str(readable_hash))
                f.close()
            time.sleep(5)
            dateintrackingtable=str(datetime.now())[:10]
            originalfilenm=str(key.split('/')[-1:])[1:-1]
            TrackingTableInsertSQL="INSERT INTO M_TRACK_BK_FILE_UPLOADS(DATE,DATASET_INSTANCE_ID,SOURCE_FILE_NAME,ORIGINAL_FILE_NM,UPDATED_FILE_NM,CONTROL_FILE_NM,SFTP_UPLOAD_FILE_FLG,NUM_OF_FILES_UPLOADED,CREATE_USER,CREATE_DT,UPDATE_USER,UPDATE_DT) VALUES('"+str(dateintrackingtable)+"',?,'"+str(FILE_MASK)+"',"+ str(originalfilenm)+",'"+str(newfilename)+"','"+str(CONTROL_FILE_NAME)+"',0,"+str(no_of_files)+",SYSTEM_USER,GETDATE(),SYSTEM_USER,GETDATE())"
            cur.execute(TrackingTableInsertSQL,60601)
            cur.commit()

    except Exception as e:
        print(e)
        deletefilesfromlocal()              

def file_size_in_Bytes(fpath):
    statinfo = os.stat(fpath)
    return (statinfo.st_size)

def uploadtosftp():      
    try:          
        WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES="\\hscsqldtci01t\Work\dtcidigital\manifests".replace('\\','\\\\')          
        S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES=WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES+'\\WFI_MANIFEST_'+str(57675)+'\\Workflow\\TMP\\'         
        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)   
        cur=sqlconnection()     
        #time.sleep(180) 
        if(1==0):                      
            client = paramiko.SSHClient()                      
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())                      
            client.connect('aws-sftp.quaero.com', username='anuj', password='quaerO@123')                      
            sftp = client.open_sftp()                  
        if(1==1):                      
            key = paramiko.RSAKey.from_private_key_file("\\\\hscsqldtci01t\\Work\\dtcidigital\\libs\\AWS_Test_SFTP\\aws_test_sftp.pem",password="quaerO@123") 
            client = paramiko.SSHClient()                      
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())                      
            client.connect('aws-sftp.quaero.com', username='anuj', pkey=key)                      
            sftp = client.open_sftp()              
        for file in filesincurrentdir:              
            if(file[-3:]=="zip" or file[-4:]=="ctrl" or file[-3:]=="fin" or file[-3:]==".gz" or file[-8:]==".trigger"):
                print("about to upload to sftp")                  
                currentfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,file)                                 
                sftpdestination='/incoming/quaerotest/'+file
                print("Destination is:"+sftpdestination)                  
                sftp.put(currentfilepath,sftpdestination)
                if(file[-3:]=="zip" or file[-3:]==".gz"):
                    UPDATESFTPUploadflgSQL="UPDATE M_TRACK_BK_FILE_UPLOADS SET SFTP_UPLOAD_FILE_FLG=1,UPDATE_USER=SYSTEM_USER,UPDATE_DT=GETDATE() WHERE UPDATED_FILE_NM='"+file+"' AND DATASET_INSTANCE_ID="+str(60601)
                    cur.execute(UPDATESFTPUploadflgSQL)
                    cur.commit()
    except Exception as e:
        deletefilesfromlocal()     
                                
def deletefilesfromlocal():      
    try:          
        WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES="\\hscsqldtci01t\Work\dtcidigital\manifests".replace('\\','\\\\')          
        S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES=WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES+'\\WFI_MANIFEST_'+str(57675)+'\\Workflow\\TMP\\'          
        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)          
        for files in filesincurrentdir:              
            totalpath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,files)              
            os.remove(totalpath)      
    except Exception as e:          
        print(e) 

def downloadfilesforwhichuploadfailed(files_not_uploaded_to_sftp):
    try:
        TOTAL_FILE_NAME_WITH_ZIP=''
        CONTROL_FILE_NAME=''            
        WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES="\\hscsqldtci01t\Work\dtcidigital\manifests".replace('\\','\\\\')
        S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES=WF_MANIFEST_PICKUP_DIRECTORY_WITH_SLASHES+'\\WFI_MANIFEST_'+str(57675)+'\\Workflow\\TMP\\'
        s3conn=boto3.resource('s3',aws_access_key_id='AKIAIGXHINAUWNKD222Q',aws_secret_access_key='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',region_name='us-east-1',)
        my_bucket = s3conn.Bucket('quaero-snowflake-dis')
        keys=[] 
        tmp = "s3://quaero-snowflake-dis/TEST/DIS_MASTERING/FRM/DB_SEGMENT_EXTRACT_BK/WFI_57538/disney_25067_2020228.txt.gz".split("/")[:-1]          
        prefilter = "/".join(tmp[3:])          
        prefilter = prefilter + "/" 

        for object_summary in my_bucket.objects.filter(Prefix=prefilter):              
            filedirpath=object_summary              
            filenamelist=str(filedirpath).split(',')[-1:]              
            filelocins3=str(filenamelist).split('\'')[1]
            if(str(filelocins3.split('/')[-1:])[2:-2] in files_not_uploaded_to_sftp):            
                keys.append(filelocins3)
            else:
                continue          
        #del keys[0]

        for key in keys:              
            localfullpath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])              
            my_bucket.download_file(key,localfullpath)              
            cur=sqlconnection()              
            cur.execute("SELECT CONTROL_FILE_FLG,CONTROL_FILE_EXT,CONTROL_FILE_MASK,SOURCE_FILE_MASK FROM M_SOURCE_ENTITY WHERE DATASET_ID= {0}".format(1000000594))              
            rows=cur.fetchall()              
            for row in rows:                  
                CONTROL_FILE_FLG=row.CONTROL_FILE_FLG                  
                CONTROL_FILE_EXT=row.CONTROL_FILE_EXT                  
                CONTROL_FILE_MASK=row.CONTROL_FILE_MASK                  
                SRC_FILE_MASK=row.SOURCE_FILE_MASK              
            FILE_MASK=SRC_FILE_MASK.replace('%','')              
            if(0==1):                  
                if(1==1):                      
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDDHHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD_HHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[:8]+'_' +str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[8:]                         
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                                                
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD"):                          
                        currentDT = datetime.today().strftime("%Y-%m-%d")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="MMDDYYYY"):                          
                        currentDT = datetime.today().strftime("%m-%d-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                    if("YYYYMMDD_HHMMSS"=="DDMMYYYY"):                          
                        currentDT = datetime.today().strftime("%d-%m-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                             
                    TOTAL_FILE_NAME_WITH_ZIP=FILE_MASK+'_'+TIMESTAMP+'.zip'                                            
                else:                      
                    TOTAL_FILE_NAME_WITH_ZIP=FILE_MASK+'.zip'                  
                zipfullpath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,TOTAL_FILE_NAME_WITH_ZIP)                  
                zip_file = zipfile.ZipFile(zipfullpath, 'w')                  
                os.chdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                
                zip_file.write(newfilename, compress_type=zipfile.ZIP_DEFLATED)              
            else:                  
                if(1==1):                      
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDDHHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD_HHMMSS"):                          
                        currentDT = datetime.now()                          
                        TIMESTAMP=str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[:8]+'_' +str(currentDT)[:19].replace('-','').replace(':','').replace(' ','')[8:]                         
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                       
                    if("YYYYMMDD_HHMMSS"=="YYYYMMDD"):                          
                        currentDT = datetime.today().strftime("%Y-%m-%d")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                           
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="MMDDYYYY"):                          
                        currentDT = datetime.today().strftime("%m-%d-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                           
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                    if("YYYYMMDD_HHMMSS"=="DDMMYYYY"):                          
                        currentDT = datetime.today().strftime("%d-%m-%Y")                          
                        TIMESTAMP=str(currentDT).replace('-','')                          
                        filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                        if(".txt.gz"==".csv"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.csv'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)                          
                        if(".txt.gz"==".txt"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath)
                        if(".txt.gz"==".txt.gz"):                              
                            newfilename=FILE_MASK+'_'+TIMESTAMP+'.txt.gz'                              
                            totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                            totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                            os.rename(totoldfilepath,totnewfilepath) 
                else:
                    filesincurrentdir=os.listdir(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES)                          
                    if(".txt.gz"==".csv"):                              
                        newfilename=FILE_MASK+'.csv'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)
                    if(".txt.gz"==".txt.gz"):                              
                        newfilename=FILE_MASK+'.txt.gz'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)                           
                    if(".txt.gz"==".txt"):                              
                        newfilename=FILE_MASK+'.txt'                              
                        totoldfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,key.split('/')[-1])                              
                        totnewfilepath=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,newfilename)                              
                        os.rename(totoldfilepath,totnewfilepath)              
            time.sleep(10)          
            if(1==1):                  
                CONTROL_FILE_NAME=CONTROL_FILE_MASK.split('%')[0]+'_'+TIMESTAMP+CONTROL_FILE_EXT                  
                CONTROL_FILE_FULL_PATH=os.path.join(S3_FILE_DOWNLOAD_LOCAL_PATH_WITH_SLASHES,CONTROL_FILE_NAME)                  
                f=open(CONTROL_FILE_FULL_PATH, 'w')
                f.write("FILE="+newfilename)
                size_in_KB=file_size_in_KB(totnewfilepath)
                f.write("\nSIZE="+str(size_in_KB))
                with open(totnewfilepath,'rb') as f1:
                    bytes=f1.read()
                    readable_hash=hashlib.md5(bytes).hexdigest()
                print("MD5SUM="+str(readable_hash))
                f.write("\nMD5SUM="+str(readable_hash))
                f.close()              
            time.sleep(5)
            originalfilenm=str(key.split('/')[-1:])[2:-2]
            UPDATETrackingTableforfailed="UPDATE M_TRACK_BK_FILE_UPLOADS SET UPDATED_FILE_NM='"+str(newfilename)+"',CONTROL_FILE_NM='"+str(CONTROL_FILE_NAME)+"',UPDATE_USER=SYSTEM_USER,UPDATE_DT=GETDATE() WHERE SOURCE_FILE_NAME='"+FILE_MASK+"' AND ORIGINAL_FILE_NM='"+originalfilenm+"' AND DATASET_INSTANCE_ID="+str(60601)
            cur.execute(UPDATETrackingTableforfailed)
            cur.commit()    
            #os.remove(totnewfilepath)
            
    except Exception as e:
        deletefilesfromlocal()


if __name__ == "__main__":            
    
    cur=sqlconnection()
    print("SQL Connection Established")
    dsi_list=[]
    cur.execute("SELECT CONTROL_FILE_FLG,CONTROL_FILE_EXT,CONTROL_FILE_MASK,SOURCE_FILE_MASK FROM M_SOURCE_ENTITY WHERE DATASET_ID= {0}".format(1000000594))              
    rows_1=cur.fetchall()              
    for row_1 in rows_1:                  
        CONTROL_FILE_FLG=row_1.CONTROL_FILE_FLG                  
        CONTROL_FILE_EXT=row_1.CONTROL_FILE_EXT                  
        CONTROL_FILE_MASK=row_1.CONTROL_FILE_MASK                  
        SRC_FILE_MASK=row_1.SOURCE_FILE_MASK              
    FILE_MASK=SRC_FILE_MASK.replace('%','')
    cur.execute("SELECT DISTINCT DATASET_INSTANCE_ID FROM M_TRACK_BK_FILE_UPLOADS WHERE SOURCE_FILE_NAME='"+FILE_MASK+"'")
    rows=cur.fetchall()
    for row in rows:
       dsi_list.append(row.DATASET_INSTANCE_ID)

    print(dsi_list)
    if 60601 not in dsi_list:
        print("Ready to download files from S3")
        downloadfroms3tolocalandzipctrl()
        print("Entering into Upload to SFTP Function")
        uploadtosftp()
    else:
        files_not_uploaded_to_sftp=[]
        SQLQueryforfailedload="SELECT ORIGINAL_FILE_NM FROM M_TRACK_BK_FILE_UPLOADS WHERE SOURCE_FILE_NAME='"+FILE_MASK+"' AND DATE='"+str(datetime.now())[:10]+"' AND SFTP_UPLOAD_FILE_FLG=0"
        cur.execute(SQLQueryforfailedload)
        rows_2=cur.fetchall()
        for row_2 in rows_2:
            files_not_uploaded_to_sftp.append(row_2.ORIGINAL_FILE_NM)
        downloadfilesforwhichuploadfailed(files_not_uploaded_to_sftp)
        uploadtosftp()
   
    deletefilesfromlocal()
			


