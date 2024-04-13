# Import all necessary libraries 
import mysql.connector
import os
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.exceptions import NotFound
import requests
import json
from datetime import datetime

# define destination table details (abstracted)
dataset = '[dataset_name]'
project_id = '[project_name]'
table_name = '[table_name]'

# load credentials 
path = os.getcwd()
os.chdir(path)
os.system('cd {}'.format(path))
os.system('pwd')
credentials = service_account.Credentials.from_service_account_file(
   '[link/to/json/file]') 
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '[link/to/json/file]' # like ./buypower-mobile-app-4567g0whk234.json for example (not a real keyfile name)
print("Credentials Loaded")

# define GBQ client
client =  bigquery.Client(project = project_id)

# define function to pull data from GBQ (destination)
def pullDataFromBQ(query):
   project_id = '[project_name]'
   df = pd.read_gbq(query, project_id=project_id)
   return df

# define table to pull data from AWS (source)
def getAWSData(host,user,password,db,query, table_name):
    mydb = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=db
    )
    df = pd.read_sql(query, mydb)
    print(f'Success fully fetched data from {table_name}')
    return df


# define function to send notification to slack channel  
def send_notification(text):
    webhook_url = '[insert slack hook here]'
    slack_data = {'text': text}

    response = requests.post(
        webhook_url, data=json.dumps(slack_data),
        headers={'Content-Type': 'application/json'})
    print(response)

# extract table schema from destination table. Because this is a simple pipeline, I have already have the table on BQ with the schema matching what we have on AWS
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

# define function to execute queries on GBQ
def bq_execute_query(query) -> object:
    '''
    This function is responsible for executing queries on Bigquery.
    
    It is used to run any query on Bigquery related to the data migration process.
    
    It takes the query and returns the results if the query ran successfully or prints the error message if the process didn't run successfully. 
    
    See example below:

    bq_execute_query("SELECT max(created_at) FROM bpcs.Msgs") -> '2023-01-01' datatype: object
    '''
    print(query)
    job_config = bigquery.QueryJobConfig()
    job_config.allow_large_results = True
    # Start the query, passing in the extra configuration.
    try:
        query_job = client.query(query, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.
        results = query_job.result()
        return results
    except Exception as e:
        print("Failed to run the query {}".format(query))
        print(e)

# define function to get the last modified dates
def get_last_modified():
    q = '''select max(created_at) last_date from `buypower-mobile-app.views.pos_terminals`'''
    last_upload_date = bq_execute_query(q)
    row = list(last_upload_date)[0]
    print(row[0])
    if row[0] is None:
        print("New table ")
        return "2010-01-01"
    else:
        return row[0]

# our main function
def main():
    send_notification('Processing POS terminal data ✅ at {}'.format(datetime.now()))
    last_updated = get_last_modified()
    print('successfully got the last_modified date :',last_updated)
    # query to pull data from aws
    query =f'''select * from [dataset].[table_name] where created_at > "{last_updated}"'''#.format(last_date)
    data = getAWSData("[connection_string]",
                        "[user]",
                        "[password]",
                        "[source_database_name]",
                        query,
                        table_name
                        )
    if len(data)>0:
        print(len(data))
        send_notification('Found {} record of POS terminal data ✅✅ '.format(len(data)))
        # load the data to GBQ
        data.to_gbq('views.pos_terminals', project_id='buypower-mobile-app', table_schema= extract_schema(), if_exists='append', chunksize=len(data)//4)
        send_notification('Successfully Added POS terminal data to BQ ✅✅✅ at {}'.format(datetime.now()))
    else:
        send_notification('Successfully, No new POS terminal data ✅✅ at {}'.format(datetime.now())) 

main()
