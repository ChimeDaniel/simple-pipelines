# Import all necessary libraries 
import re
import pandas as pd
import requests
import certifi
import os
import json
from datetime import datetime, tzinfo, timezone
from pymongo import MongoClient
from google.oauth2 import service_account
from datetime import datetime, timedelta, date
from google.cloud import bigquery

# define destination table details (abstracted)
dataset = '[dataset_name]'
project_id = '[project_name]'
table_name = '[table_name]'

# load credentials 
credentials = service_account.Credentials.from_service_account_file(
    '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]' # like ./buypower-mobile-app-4567g0whk234.json for example (not a real keyfile name)

# define regex function to remove ascii
def remove_ascii_0( df):
    headers_regex_pattern = '[^!-~]'
    regex = re.compile(headers_regex_pattern)

    def sol(x):
        try:
            if x:
                return regex.sub('',x)
            else:
                return None
        except:
            return regex.sub('',str(x))
    for column in df.select_dtypes(include='object'):
        df[column] = df[column].apply(sol )
    return df

# define function to send notification to slack channel  
def send_notification(text):
    webhook_url = '[insert slack hook here]'
    slack_data = {'text': text}

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'})
    print(response)

# extract table schema from destination table. Because this is a simple pipeline, I have already created an empty table on BQ with the schema matching what we have on MongDB
def extract_schema(): 
    dataset_id = dataset
    table_id = table_name

    client =  bigquery.Client(project = project_id)
    dataset_ref = client.dataset(dataset_id, project= project_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)  # API Request

    schema = []
    for x in table.schema:
        t1 = {}
        t1[x.name] = x.field_type
        schema.append(t1)

    new_schema = [{'name': list(x.keys())[0], 'type':x[list(x.keys())[0]]} for x in schema]
    return new_schema

# define function to get the last modified dates
def get_last_modified():
    data = pd.read_gbq(f'select max(created) last_modified from {dataset}.{table_name} ', project_id = project_id)
    data.fillna(0, inplace=True)
    max_date = data['last_modified'][0].to_datetime64()
    max_datetime = pd.to_datetime(max_date)
    return max_datetime

# define function to request json
def requests_json(json_data):
    return json.JSONEncoder(default= str).encode(json_data)

# our main function
def main():
    send_notification('Processing MongoDB Vend_log_main data ✅ ✅ at {}'.format(datetime.now()))
    start_date = get_last_modified()
    schema = extract_schema()
    
    # MongoDB query to get data from last modified date
    ca = certifi.where()
    client = MongoClient('mongodb+srv://...', tlsCAFile=ca)
    filter={
        'created': {
        "$gt":start_date
        }
    }
    result = client['liveDB']['vend_log'].find(
    filter=filter
    )
    res = result 
    data = pd.DataFrame(list(res))

    if len(data):
        data['id'] = data._id.apply(lambda x: str(x))

        for col in ['id','ref', 'action','success', 'gateway', 'created','service', 'request', 'response']:
            if col not in data:
                data[col] = None

        data = data[['id','ref', 'action','success', 'gateway', 'created','service', 'request', 'response' ]]

        data.request = data.request.apply(requests_json)
        data.response = data.response.apply(lambda x: json.dumps(x))
        data = remove_ascii_0(data)

        # load the data to GBQ
        data.to_gbq('{}.{}'.format(dataset,table_name), project_id='buypower-mobile-app', credentials = credentials,if_exists='append', progress_bar=True, table_schema = schema, chunksize =len(data)//4)
        send_notification('Successfully added Vend_logs data ✅ at {}'.format(datetime.now()))
    else:
        send_notification('Vend_logs data up to date ✅ at {}'.format(datetime.now()))

main()
