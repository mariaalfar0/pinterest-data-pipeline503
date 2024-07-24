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


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print("Pin result:")
            print(pin_result)
            print("Geo result:")
            print(geo_result)
            print("User result:")
            print(user_result)


pin_invoke_url = "https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.pin"
pin_df = {}
pin_data = json.dumps({
    "records": [
        {      
        "value": {"index": pin_df["index"], "unique_id": pin_df["unique_id"], "title": pin_df["title"], 
                  "description": pin_df["description"], "poster_name": pin_df["poster_name"],
                  "follower_count": pin_df["follower_count"], "tag_list": pin_df["tag_list"],
                  "is_image_or_video": pin_df["is_image_or_video"], "image_src": pin_df["image_src"], 
                  "downloaded": pin_df["downloaded"], "save_location": pin_df["save_location"]}
        }
    ]
})

geo_invoke_url = "https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.geo"
geo_df = {}
geo_data = json.dumps({
    "records": [
        {      
        "value": {"ind": geo_df["ind"], "timestamp": geo_df["timestamp"], 
                  "latitude": geo_df["latitude"], "longitude": geo_df["longitude"], 
                  "country": geo_df["country"]}
        }
    ]
})

user_invoke_url = "https://ez41mcd5n4.execute-api.us-east-1.amazonaws.com/0affe2a66fdf-stage/topics/0affe2a66fdf.user"
user_df = {}
user_data = json.dumps({
    "records": [
        {      
        "value": {"ind": user_df["ind"], "first_name": user_df["first_name"], 
                  "last_name": user_df["last_name"], "age": user_df["age"], 
                  "date-joined": user_df["date_joined"]}
        }
    ]
})


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
    
    


