import snowflake
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect 
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


SnowflakeDialect.supports_statement_cache = True
class SnowflakeCredentialError(Exception):
    def __init__(self, *args: object) -> None:
        self.message = 'Either password or authenticator needs to be provided in credential.yml'
    
    def __str__(self) -> str:
        return self.message
    
class SnowflakeConnector(object):
    
    def __init__(self, ds):
        self.account=ds.get('account')
        self.user=ds.get('user')
        self.database=ds.get('database')
        self.schema=ds.get('schema')
        self.role=ds.get('role')
        self.warehouse=ds.get('warehouse')
        self.auth=ds.get('authenticator')
        self.stage=ds.get('stage')
        self.password=ds.get('password')
  
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
        }

        if self.password:
            db_parameters['password'] = self.password
        if self.auth:
            db_parameters['authenticator'] = self.auth            
        return URL(**db_parameters, cache_column_metadata=True)
    
    def verify_connection(self):
        return None
    
    @staticmethod
    def validate(credentials):
        authenticator = credentials.get('authenticator')
        password = credentials.get('password')
        if any([authenticator, password]):
            return True
        raise SnowflakeCredentialError()
  
