import snowflake
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect 
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# Snowflake connection parameters
snowflake_account = 're61407.ap-southeast-2'
snowflake_user = 'chenxuan.rong'
snowflake_database = 'zz_chenxuan_rong_dev'
snowflake_schema = 'sandbox'
snowflake_warehouse = 'warehouse_xs'
snowflake_auth = 'externalbrowser'
snowflake_role="data_dept_property_dev"

SnowflakeDialect.supports_statement_cache = True
db_parameters = {
    "account": snowflake_account,
    "user": snowflake_user,
    "database": snowflake_database,
    "schema": snowflake_schema,
    "warehouse": snowflake_warehouse
}

class SnowflakeConnector():
    
    def __init__(self, account, user, database, schema, warehouse, auth):
        self.account=account
        self.user=user
        self.database=database
        self.schema=schema
        self.warehouse=warehouse
        self.auth=auth
  
    @staticmethod
    def connector():
        conn = snowflake.connector.connect(
            user=snowflake_user,
            account=snowflake_account,
            warehouse=snowflake_warehouse,
            database=snowflake_database,
            schema=snowflake_schema,
            authenticator=snowflake_auth,
            role=snowflake_role,
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
            "account": snowflake_account,
            "user": snowflake_user,
            "database": snowflake_database,
            "warehouse": snowflake_warehouse,
            "role": snowflake_role,
            "authenticator": snowflake_auth,
        }
        return URL(**db_parameters, cache_column_metadata=True)
    
    def verify_connection(self):
        return None