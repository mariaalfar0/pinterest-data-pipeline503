import db_endpoints
import json
import requests
import sqlalchemy
from db_utils import AWSDBConnector
from sqlalchemy import text


new_connector = AWSDBConnector()

def extract_data(engine: sqlalchemy.engine, table_name: str, counter: int) -> dict:
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
    
def post_to_api(invoke_url: str, stream_name: str, data):
    """
    The function `post_to_api_batch` sends a POST request to a specified URL with data formatted for a Kafka
    API.
    
    :param invoke_url: The `invoke_url` parameter is the URL where you want to send a POST request with 
        the data. This URL should be the endpoint of the API you are trying to interact with.
    :param data: The `data` parameter is the information that you want to send to the API. It could be 
        any data that you need to post to the specified API endpoint. This data
        will be included in the payload of the request as part of the JSON body.
    """
    # NOTE could add a try except statement here
    payload = json.dumps({"StreamName": stream_name,"Data": data, "PartitionKey": "partition=1"}, default = str)

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
    This Python function sends data from different tables in a database to three different API endpoints
    in a loop.
    """
    counter = 3194
    engine = new_connector.create_db_connector()

    row_count = query_db(engine, "SELECT COUNT(*) AS count FROM pinterest_data")
    max_row_count = row_count["count"]
    
    while counter <= max_row_count:
        pin_result = extract_data(engine, "pinterest_data", counter)
        geo_result = extract_data(engine, "geolocation_data", counter)
        user_result = extract_data(engine, "user_data", counter)
 
        post_to_api(db_endpoints.STREAM_API_PIN_ENDPOINT, db_endpoints.STREAM_API_PIN_STREAMNAME, pin_result)
        post_to_api(db_endpoints.STREAM_API_GEO_ENDPOINT, db_endpoints.STREAM_API_GEO_STREAMNAME, geo_result)
        post_to_api(db_endpoints.STREAM_API_USER_ENDPOINT, db_endpoints.STREAM_API_USER_STREAMNAME, user_result)

        counter = counter + 1
        print(f"{counter} out of {max_row_count}")     
       

if __name__ == "__main__":
    send_to_api()

    
    


