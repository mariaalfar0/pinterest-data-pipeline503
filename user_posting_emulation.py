import db_endpoints
import json
import requests
import sqlalchemy
import yaml
from db_utils import AWSDBConnector
from sqlalchemy import text

# This class serves as a connector to an AWS database.
class AWSDBConnector:

    def __init__(self):
        """
        The function initializes database connection parameters by loading credentials from a YAML file.
        """
        with open('db_creds.yaml') as f:
            db_creds = yaml.load(f, Loader=yaml.SafeLoader)
        self.HOST = db_creds['RDS_HOST']
        self.USER = db_creds['RDS_USER']
        self.PASSWORD = db_creds['RDS_PASSWORD']
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        """
        The function creates a database connector using SQLAlchemy to connect to a MySQL database.
        :return: An SQLAlchemy engine object for connecting to a MySQL database is being returned.
        """
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def extract_data(engine, table_name, counter):
    """
    This function extracts data from a specified table in a database using the provided SQLAlchemy
    engine, limiting the number of rows returned by the specified counter.
    
    :param engine: The `engine` parameter is typically an instance of a database engine, such as
        SQLAlchemy's `create_engine` function. It represents the connection to the database where the data
        is stored
    :param table_name: The `table_name` parameter in the `extract_data` function is used to specify the
        name of the table from which you want to extract data
    :param counter: The `counter` parameter in the `extract_data` function is used to specify the
        starting point for selecting rows from the database table. It determines the offset at which the
        query will start retrieving rows from the table
    :return: The function `extract_data` returns a dictionary containing the data from the specified
        table in the database. The data is retrieved by executing a SQL query to select a single row from
        the table with a specified limit (counter). The function returns the data of the selected row as a
        dictionary.
    """
    
    with engine.connect() as connection:
        selected_string = text(f"SELECT * FROM {table_name} LIMIT {counter}, 1")
        selected_row = connection.execute(selected_string)
            
        for row in selected_row:
            result = dict(row._mapping)
        
        return result
    
def post_to_api(invoke_url, data):
    """
    The function `post_to_api` sends a POST request to a specified URL with data formatted for a Kafka
    API.
    
    :param invoke_url: The `invoke_url` parameter is the URL where you want to send a POST request with 
        the data. This URL should be the endpoint of the API you are trying to interact with.
    :param data: The `data` parameter is the information that you want to send to the API. It could be 
        any data that you need to post to the specified API endpoint. This data
        will be included in the payload of the request as part of the JSON body.
    """
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
    """
    The `query_db` function executes a SQL query using the provided engine and returns the result as a
    dictionary.
    
    :param engine: The `engine` parameter in the `query_db` function is typically an SQLAlchemy engine
        object that represents a connection to a database. This engine object is used to establish a
        connection to the database and execute queries
    :param query: The `query_db` function takes two parameters: `engine` and `query`. The `engine`
        parameter is the database engine object used to establish a connection to the database. The `query`
        parameter is the SQL query that you want to execute on the database using the provided engine
    :return: The function `query_db` is returning a dictionary `result` that contains the data from the
        last row of the query result.
    """
    with engine.connect() as connection:
        query = text(f"{query}")
        run_query = connection.execute(query)
        
        for row in run_query:
            result = dict(row._mapping)
        
        return result

def send_to_api():
    """
    This Python function sends data from different tables in a database to specific API endpoints in a
    loop.
    """
    counter = 10492
    engine = new_connector.create_db_connector()

    row_count = query_db(engine, "SELECT COUNT(*) AS count FROM pinterest_data")
    max_row_count = row_count["count"]
    
    while counter <= max_row_count:
        pin_result = extract_data(engine, "pinterest_data", counter)
        geo_result = extract_data(engine, "geolocation_data", counter)
        user_result = extract_data(engine, "user_data", counter)
 
        post_to_api(db_endpoints.BATCH_API_PIN_ENDPOINT, pin_result)
        post_to_api(db_endpoints.BATCH_API_GEO_ENDPOINT, geo_result)
        post_to_api(db_endpoints.BATCH_API_USER_ENDPOINT, user_result)

        counter = counter + 1
        print(f"{counter} out of {max_row_count}")     
       

if __name__ == "__main__":
    send_to_api()

    
    


