import psycopg2
import configparser, boto3, pymysql, csv

# get Redshift credentials
parser=configparser.ConfigParser()
parser.read('pipeline.conf')
dbname = parser.get("cluster_config", "database")
user = parser.get("cluster_config", "username")
password = parser.get("cluster_config", "password")
host = parser.get("cluster_config", "host")
port = parser.get("cluster_config", "port")



# connect to cluster

rs_conn=psycopg2.connect(
	"dbname="+dbname
	+" user="+user
	+" password="+password
	+" host="+host
	+" port="+port)


# import redshift_connector
# rs_conn = redshift_connector.connect(
#      host=host,
#      database=dbname,
#      port=int(port),
#      user=user,
#      password=password
#   )

rs_sql="""SELECT COALESCE(MAX(LastUpdated), '1901-01-01') FROM Orders;"""

rs_cursor=rs_conn.cursor()
rs_cursor.execute(rs_sql)
result=rs_cursor.fetchone()

print(result)

# there's only one row and column returned
## HERE FOR POSTGRES I CHANGED TO result FROM result[0], WAS GIVING ERROR TypeError: 'datetime.datetime' object does not support indexing
last_updated_warehouse=result
print(last_updated_warehouse)

# close cursor and commit the transaction
rs_cursor.close()
rs_conn.close()


# close connection
rs_conn.close()


# get MySQL info and connect
hostname = parser.get("postgres_config", "hostname")
port = parser.get("postgres_config", "port")
username = parser.get("postgres_config", "username")
dbname = parser.get("postgres_config", "database")
password = parser.get("postgres_config", "password")

conn = psycopg2.connect("dbname="+dbname+" user="+username+" password="+password+" host="+hostname+" port="+port)

m_query="""SELECT *
FROM Orders
WHERE LastUpdated > %s;"""

local_filename="order_extract_postgres.csv"


m_cursor=conn.cursor()

# Extraction
#m_cursor.execute('''use BOOK''')
m_cursor.execute(m_query, (last_updated_warehouse))
results=m_cursor.fetchall()
print(results)


# Saving csv file locally
with open(local_filename, 'w') as fp:
	csv_w=csv.writer(fp, delimiter='|')
	csv_w.writerows(results)

fp.close()
m_cursor.close()
conn.close()


# Uploading csv to S3 bucket
# get credentials

#parser=configparser.ConfigParser()
#parser.read('pipeline.conf')
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3=boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file=local_filename

s3.upload_file(local_filename, bucket_name, s3_file)