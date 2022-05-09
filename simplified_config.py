# Importing Required Modules

import sys
import yaml
from yaml.loader import SafeLoader
import json
import boto3
import random, time

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



# Referred from https://keestalkstech.com/2021/03/python-utility-function-retry-with-exponential-backoff/

def retry_with_backoff(fn, retries = 10, backoff_in_seconds = 600):
  x = 0
  while True:
    try:
      s3 = boto3.client('s3')
      s3.put_object(
           Body=json.dumps(final_config),
           Bucket='your_bucket_name',
           Key='your_key_here'
      )
    except:
      if x == retries-1:
        raise
      else:
        sleep = (backoff_in_seconds * 2 ** x + 
                 random.uniform(0, 1))
        time.sleep(sleep)
        x += 1
#retry_with_backoff(fn, retries = 10, backoff_in_seconds = 600)


'''Note: We can also use the method s3.upload_file to upload the json file by provinding the path using the function below.
   response = s3.upload_file(config_file.json,'my_bucket_name',json_data.json) '''
