import snowflake.connector
import configparser

parser = configparser.ConfigParser()
parser.read('pipeline.conf')
account_name = parser.get("snowflake_creds", "account_name")
username = parser.get("snowflake_creds", "username")
password = parser.get("snowflake_creds", "password")
database = parser.get("snowflake_creds", "database")
schema = parser.get("snowflake_creds", "schema")
warehouse = parser.get("snowflake_creds", "warehouse")

snow_conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account_name,
    database=database,
    schema=schema,
    warehouse=warehouse)

sql = """copy into snow_db_s3.public.Orders
from @my_s3_stage/order_extract.csv;"""

cur = snow_conn.cursor()

# Extraction
cur.execute(sql)
cur.close()

print('done')
