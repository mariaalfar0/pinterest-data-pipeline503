class AWSDBConnector:

    def __init__(self):
        """
        The function initializes database connection parameters by loading 
        credentials from a YAML file.
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