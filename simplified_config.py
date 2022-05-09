# Importing Required Modules
import sys
import yaml
from yaml.loader import SafeLoader
import json
import boto3
from botocore.exceptions import NoCredentialsError
import time
from pathlib import Path

''' Checking the correct number of arguments are passed for the script 
    to run successfully'''
total_args = len(sys.argv)-1
if (total_args != 3):
  print("Expected 3 parameters, received ", total_args, "Please ensure you pass the environment, region and the path.")
  sys.exit()

# Variable initialisation and Declaration 
global env, region, path, dict1, dict2, dict3
env=sys.argv[1]
region=sys.argv[2]
path=sys.argv[3]

# Function to fetch the compressed json file
def compressed_file(path):
    with open(path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        dict1={}
        dict2={}
        dict3={}
        dict2["service_name"]=data['metadata']['service']
        dict2["team_name"]=data['metadata']['team']
        dict2["cost_center"]=data['metadata']['cost_center']
        dict1["environment"] = env
        dict1["region"] = region
        dict1["configuration"] = dict2
        dict2={}
        if(env in data.keys() and region in data[env].keys()):
            for i in sorted(data['common']):
              dict2=data['common'][i]
              for j in sorted(dict2.keys()):
                dict3[j]=dict2[j]
              if i in data[env][region].keys():
                  for k in data[env][region][i].keys():
                    dict3.update({k:data[env][region][i][k]})
              dict1[i]=dict3
              dict3={}
        else:
          for i in sorted(data['common']):
            dict2=data['common'][i]
            for j in sorted(dict2.keys()):
              dict3[j]=dict2[j]
            dict1[i]=dict3
            dict3={}
        return dict1

final_config = compressed_file(path)
final_config = json.dumps(final_config)
print(final_config)


base = Path('/home/ec2-user/')  
jsonpath = base / "config.json" 
base.mkdir(exist_ok=True) #Create the directory if it does not exist and write json file:
jsonpath.write_text(json.dumps(final_config))
local_file=str(jsonpath)  
s3_file='config_s3.json' 
bucket='my-bucket1'  


# Function to Upload file to S3 bucket
def upload_to_aws(local_file, bucket, s3_file):
	s3 = boto3.client('s3')
	print("Trying to upload")
	s3.upload_file(local_file, bucket, s3_file)
	print("Upload Successful")


# Exponential Backoff Retry logic for Uploading
def retry(func, max_tries=30):
    for i in range(max_tries):
        try:
           time.sleep(20) 
           func(local_file, bucket, s3_file)
           break
        except Exception:
            continue

# Function call for upload and retry
retry(upload_to_aws)
