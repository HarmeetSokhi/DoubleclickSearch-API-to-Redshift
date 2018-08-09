#!/usr/bin/python
#****************************************************************************************
#    DS3  Python Script for Ds3
#    ----------------------------------
#
#    Notes:
#
#    1. This script fetches data use DS3 Google API 
#       You can see the variables below for the details of the API
#    2. Script take JSON as a input from the config_file
#    3. Once the data is received it writes to S3 (AWS).Please see the Variables for
#       the path details
#    4. Once Data is written to S3 it writes it to the Redshift staging table.Staging
#       table is truncated before writing the data
#    5. Data is inserted into Master table after the above step
#
#    Version:
#      Please check Git for Change Management & Details.
#
#    Author:
#      Harmeet Sokhi
#
#
#    Date :
#      15/02/2018
#*******************************************************************************************
import httplib2
import pprint
import simplejson
import time
import json
import psycopg2
import boto3
import boto
import os
import botocore
import datetime
import sys

from googleapiclient.errors import HttpError
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import OAuth2Credentials
from oauth2client import GOOGLE_AUTH_URI

from oauth2client import GOOGLE_REVOKE_URI
from oauth2client import GOOGLE_TOKEN_URI


### Jenkins Passowrds  
REFRESH_TOKEN = os.environ['REFRESH_TOKEN']
CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
PWD = os.environ['Admin_DBPWD'] 

# Jenkins Parameters
AgencyID =  os.environ['AgencyID']
AdvertiserID = os.environ['AdvertiserID']

# Progaram Variables
API_NAME = "doubleclicksearch"
API_VERSION = "v2"
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
Config_File = 'config.json.ds3'
DBNAME = 'ENTER DATABASE NAME'
HOST = 'XXXENTER YOUR REDHSIFT URL HERE'
PORT = 5439
USER = 'ENTER DATABASE USER ID'
SCHEMA = 'ENTER SCHEMA HERE'
Staging_Table  = 'NAME OF THE STAGING TABLE'
Master_Table =  'NAME OF THE MASTER TABLE'
AWS_IAM_ROLE = "ROLE TO PUT DATA FROM S3 TO REDHSIFT"
S3_FilePath = "S3 FILE PATH"
EC2_File_Path = "INETRMEDIATE FILE PATH"
reportName = 'reportname'
OAUTH_SCOPE = ['https://www.googleapis.com/auth/doubleclicksearch']

def authorization():
	try:
	        credentials = OAuth2Credentials(None, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, None,
        	                               GOOGLE_TOKEN_URI, None,
                	                       revoke_uri=GOOGLE_REVOKE_URI,
                        	               id_token=None,
                                	       token_response=None)

	        # Create an httplib2.Http object and authorize it with  credentials
	        http = httplib2.Http()
	        http = credentials.authorize(http)
		return http	
	except Exception as e:
		pprint.pprint(" >> Error in authorization: %s" %e)
		sys.exit(1)
	
def gen_report(configfile,http):
        with open(configfile,'r') as f:
             bodystr = f.read()
        bodyvar = bodystr % (AgencyID,AdvertiserID,startDATE,endDATE)
	print 'Json: ' + bodyvar
        body = simplejson.loads(bodyvar)
        SERVICE = build(API_NAME, API_VERSION, http=http)
        request = SERVICE.reports().request(body=body)
        json_data = request.execute()
	print ' >> Report generated. Report ID ' + json_data['id']
        return [json_data['id'], SERVICE]

def poll_report(SERVICE, report_id,con):
  #Poll the API with the reportId until the report is ready, up to ten times.
  #  Args:
  #  service: An authorized Doublelcicksearch service.
  #  report_id: The ID DS has assigned to a report.

  for _ in xrange(10):
    try:
      request = SERVICE.reports().get(reportId=report_id)
      json_data = request.execute()
      if json_data['isReportReady']:
        print ' >> The report is ready.'

        # For large reports, DS automatically fragments the report into multiple
        # files. The 'files' property in the JSON object that DS returns contains
        # the list of URLs for file fragment. To download a report, DS needs to
        # know the report ID and the index of a file fragment.
        for i in range(len(json_data['files'])):
           print ' >> Downloading fragment ' + str(i) + ' for report ' + report_id
	   fetchreport(SERVICE, report_id, str(i),con) 
        return

      else:
         print ' >> Report is not ready. I am trying again...'
         time.sleep(10)
    except HttpError as e:
      error = simplejson.loads(e.content)['error']['errors'][0]
      # See Response Codes
      pprint.pprint(' >> HTTP code %d, reason %s' % (e.resp.status, error['reason']))
      sys.exit(1)
      break

def fetchreport(SERVICE,report_id,report_fragment,con):
	global reportName
	#Generate and print sample report.
	
	#  Args:
	#    service: An authorized Doublelcicksearch service.
	#    report_id: The ID DS has assigned to a report.
	#    report_fragment: The 0-based index of the file fragment from the files array.

	if startDATE == endDATE:
		reportName = reportName + '_' + startDATE + '_' + report_fragment + '.csv'
		file_in_s3 = S3_FilePath + startDATE + '/' + reportName 
	else:
		reportName = reportName + '_' + startDATE + '_' + endDATE + '_' + report_fragment + '.csv'
		file_in_s3 = S3_FilePath + startDATE + '...' + endDATE + '/' + reportName + '_' + report_fragment + '.csv'

	repfile = EC2_File_Path + reportName
	f = file(repfile, 'w')
	request = SERVICE.reports().getFile( reportId=report_id, reportFragment=report_fragment)
	f.write(request.execute())
	f.close()

        f = open(repfile, "r")
	counter = 0
	for line in f.read().split('\n'):
	        counter = counter + 1
	counter = counter - 2
	f.close()

	print ' >> Filename in EC2 : ' + repfile 
	print ' >> Total records in File: ' + str(counter)

	FilePath = write_to_s3(repfile,"BUCKETNAME",file_in_s3)	
   #    os.remove(repfile)
	print ' >> File written in S3. File Path is : ' + FilePath
	write_to_db(FilePath,con)

def  write_to_s3(file_to_upload,bucket,file_name_in_s3):
	try:
		s3 = boto3.resource('s3')
		s3.Bucket(bucket).upload_file(file_to_upload,file_name_in_s3)
		
		return "s3://BUCKETNAME/" + file_name_in_s3 
	except Exception as e:
		pprint.pprint(' >> Unable to upload to S3 : %s' %e)
		sys.exit(1)
		
def  write_to_db(FILE_PATH,con):
    try:
	print ' >> Truncating the staging table '
	deletesql = " Truncate Table " + SCHEMA + "." + Staging_Table 
	cur = con.cursor()
	# drop previous days contents from the staging table
	try:
		cur.execute(deletesql)		
		print(" >> Truncate for staging executed successfully")

	except Exception as e:
		pprint.pprint(" >> Error executing truncate in the staging table: %s " %e)
		sys.exit(1)
 
       # Copy to the staging table first and if it is successful copy to the master table

        copysql="""copy {}.{} from '{}' credentials 'aws_iam_role={}' format as csv IGNOREHEADER 1""".format(SCHEMA,Staging_Table,FILE_PATH,AWS_IAM_ROLE)

    	try:
        	cur.execute(copysql)
        	print(" >> Copy Command executed successfully")
		
		# Copy to the Master Table
		sqlMaster = " Insert into " + SCHEMA + "." + Master_Table + " Select * from " + SCHEMA + "." + Staging_Table + " STG1 where not exists(Select 1 from " + SCHEMA + "." + Master_Table + " Mas1 where MAS1.Date1 =STG1.Date1)"     	
		try:
			cur.execute(sqlMaster)
			inscount=cur.rowcount
			print ' >> Rows inserted : ' + str(inscount)
		except Exception as e:
			pprint.pprint(" >> Failed to execute Insert to Master: %s " %e)
			sys.exit(1)

    	except Exception as e:
        	pprint.pprint(" >> Failed to execute copy command. Error : %s" %e)
		sys.exit(1)
    	con.close()

    except Exception as e:
        pprint.pprint(' >> Unable to connect to Redshift.Error: %s' %e)
	sys.exit(1)

def getDates(con):

	        startDATE = os.getenv('startDATE',"")
        	endDATE = os.getenv('endDATE',"")

		if startDATE == "":
			print ' >> Calculating StartDATE '
			cur = con.cursor()
		        sqlMaster = " select max(Date1) from " + SCHEMA + "." + Master_Table 
		        try:
		            cur.execute(sqlMaster)
			    print ' >> Executed the max Date query '
             		    sqlRow = cur.fetchone()
			    print ' >> Fetching the date row,if any '
	                    if sqlRow[0] == None:
			    	startDATE = ""
			    else:
				startDATE = sqlRow[0]
			    if startDATE == "":
				print ' >> No max Date row '
				yesterDate = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
				startDATE = yesterDate
			    else:
				print ' >> setting the startDATE to next day of last updated data'
				startDATE = startDATE + datetime.timedelta(days = 1)
#	           	    print ' >> startDate is: ' + startDATE
		        except Exception as e:
		            pprint.pprint(" >> Failed to get Max Date from the Master Table %s " %e)
			    sys.exit(1)
		else:
			print ' >> Settiing startDATE from Jenkins parm'
			startDATE = os.environ['startDATE']			
			
		if endDATE == "":
			print ' >> Going to calculate endDate'
			yesterDate = (datetime.datetime.now() - datetime.timedelta(2)).strftime('%Y-%m-%d')			
			endDATE = yesterDate
		else:
			print ' >> Setting the endDATE from Jenkins parm'
			endDATE = os.environ['endDATE']

		startDATE = str(startDATE)
		endDATE = str(endDATE)

		if startDATE > endDATE:
			print ' >> startDATE greate than endDATE ?? I am setting both as endDATE'
			startDATE = endDATE
		
		print ' >> startDate : ' + startDATE 	
		print ' >> endDate: ' + endDATE
		 
		return [startDATE,endDATE]


def connectDB():
        print(" >> Establishing Connection with Redshift..")
        con=psycopg2.connect(dbname= DBNAME, host=HOST, port= PORT, user= USER, password= PWD)
        print(" >> Connection Successful!")
	return con

def main():	

	global startDATE 
	global endDATE

	startDATE = os.getenv('startDATE',"")
	endDATE = os.getenv('endDATE',"")

	try:
		http = authorization()
		print ' >> Authorization Successful'        					
		con = connectDB()
		print ' >> Setting Dates '
	 	startDATE, endDATE = getDates(con)		
		print ' >> Setting JSON and generating report '
		report_id, SERVICE = gen_report(Config_File,http)
		print ' >> Polling Report '
		poll_report(SERVICE, report_id,con)
		print ' >> Data ingested successfully'

	except Exception as e:
                pprint.pprint(" >> Error in the script : %s" %e)
		sys.exit(1)
			
	
if __name__ == "__main__":				
	main()