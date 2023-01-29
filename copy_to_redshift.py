import boto3, configparser, psycopg2


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



# Uploading csv to S3 bucket
# get credentials

#parser=configparser.ConfigParser()
#parser.read('pipeline.conf')
account_id = parser.get("aws_boto_credentials", "account_id")
iam_role = parser.get("cluster_config", "iam_role")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

	
# run COPY command
file_path=("s3://"+bucket_name+"/order_extract.csv")
role_string=("arn:aws:iam::"+account_id+":role/"+iam_role)
sql="COPY public.Orders"
sql=sql+" from %s "
sql=sql+" iam_role %s;"

# create a cursor object and execute the COPY
cursor=rs_conn.cursor()
cursor.execute(sql, (file_path, role_string))

# close cursor and commit the transaction
cursor.close()
rs_conn.commit()

# close connection
rs_conn.close()

