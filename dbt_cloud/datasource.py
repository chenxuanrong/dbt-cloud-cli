import snowflake
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect 
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


SnowflakeDialect.supports_statement_cache = True
class SnowflakeConnector():
    
    def __init__(self, ds):
        self.account=ds.get('account')
        self.user=ds.get('user')
        self.database=ds.get('database')
        self.schema=ds.get('schema')
        self.role=ds.get('role')
        self.warehouse=ds.get('warehouse')
        self.auth=ds.get('authenticator')
        self.stage=ds.get('stage')
  
    @staticmethod
    def connector(ds):
        conn = snowflake.connector.connect(
            user=ds.get('user'),
            account=ds.get('account'),
            warehouse=ds.get('warehouse'),
            database=ds.get('database'),
            schema=ds.get('schema'),
            authenticator=ds.get('authenticator'),
            role=ds.get('role'),
            session_parameters={
                'QUERY_TAG': 'DQM-API'
            }
        )
        print('Connecting to snowflake')
        return conn
    
    def create_engine(self):
        return create_engine(self.to_database_url())
    
    def engine_args(self):
        return dict(session_parameters={
            'QUERY_TAG': 'DQM-API'
        })
    
    def to_database_url(self):
        db_parameters = {
            "account": self.account,
            "user": self.user,
            "database": self.database,
            "warehouse": self.warehouse,
            "role": self.role,
            "authenticator": self.auth,
        }
        return URL(**db_parameters, cache_column_metadata=True)
    
    def verify_connection(self):
        return None
    
    def validate(self):
        return None
  
