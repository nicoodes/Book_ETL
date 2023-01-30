import psycopg2
import csv, boto3, configparser

parser=configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get("postgres_config", "hostname")
port = parser.get("postgres_config", "port")
username = parser.get("postgres_config", "username")
dbname = parser.get("postgres_config", "database")
password = parser.get("postgres_config", "password")

conn = psycopg2.connect("dbname="+dbname+" user="+username+" password="+password+" host="+hostname+" port="+port)

m_cursor=conn.cursor()

# Extraction
#m_cursor.execute('''use BOOK''')
m_cursor.execute('''select * from  Orders;''')
results=m_cursor.fetchall()
print(results)

# Saving csv file locally
local_filename='order_extract_postgres.csv'
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