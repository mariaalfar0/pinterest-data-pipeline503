import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
import pymysql
import datetime
import pandas as pd


random.seed(100)


class AWSDBConnector:

    def __init__(self):
        with open('db_creds.yaml') as f:
            db_creds = yaml.load(f, Loader=yaml.SafeLoader)
        self.HOST = db_creds['RDS_HOST']
        self.USER = db_creds['RDS_USER']
        self.PASSWORD = db_creds['RDS_PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def extract_data(engine, table_name, counter):
    
    with engine.connect() as connection:
        selected_string = text(f"SELECT * FROM {table_name} LIMIT {counter}, 1")
        selected_row = connection.execute(selected_string)
            
        for row in selected_row:
            result = dict(row._mapping)
        
        return result
    
def post_to_api(invoke_url, data):
    payload = json.dumps({
        "records": [
            {      
            "value": data
            }
        ]
    }, default = str)

    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    response = requests.request("POST", invoke_url, headers=headers, data=payload)
    print("Status code of response: ")
    print(response.status_code)

def query_db(engine, query):
    with engine.connect() as connection:
        query = text(f"{query}")
        run_query = connection.execute(query)
        
        for row in run_query:
            result = dict(row._mapping)
        
        return result

def send_to_api():
    counter = 10492
    engine = new_connector.create_db_connector()

    row_count = query_db(engine, "SELECT COUNT(*) AS count FROM pinterest_data")
    max_row_count = row_count["count"]
    
    while counter <= max_row_count:
        pin_result = extract_data(engine, "pinterest_data", counter)
        geo_result = extract_data(engine, "geolocation_data", counter)
        user_result = extract_data(engine, "user_data", counter)
 
        post_to_api("https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.pin", pin_result)
        post_to_api("https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.geo", geo_result)
        post_to_api("https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.user", user_result)

        counter = counter + 1
        print(f"{counter} out of {max_row_count}")     
       

if __name__ == "__main__":
    send_to_api()

    
    


