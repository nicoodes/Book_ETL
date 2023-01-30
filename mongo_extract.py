from pymongo import MongoClient
import datetime, csv, boto3
from datetime import timedelta
import configparser

# mongo_config values
parser=configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get("mongo_config", "hostname")
collection_name = parser.get("mongo_config", "collection")
username = parser.get("mongo_config", "username")
database_name = parser.get("mongo_config", "database")
password = parser.get("mongo_config", "password")

mongo_client=MongoClient("mongodb+srv://"+username+":"+password+"@"+hostname+"/"+database_name+"?retryWrites=true&"+"w=majority&ssl=true&"+"ssl_cert_reqs=CERT_NONE")

# connect to db where collection resides
mongo_db=mongo_client[database_name]

#connect to the collection
mongo_collection=mongo_db[collection_name]


# set date range
start_date=datetime.datetime.today() + timedelta(days=-1)
end_date=start_date+timedelta(days=1)


# page 71 NOTE
# using a defined range for dates to extract here, in case I could also use the last event loaded from the warehouse
mongo_query={"$and":[
{"event_timestamp": {"$gte": start_date}},
{"event_timestamp": {"$lt": end_date}}
]}


event_docs=mongo_collection.find(mongo_query, batch_size=3000)

# blank list to store results
all_events=[]

# iterate through the cursor
for doc in event_docs:
	# include default values
	event_id=str(doc.get("event_id", -1))
	event_timestamp=doc.get("event_timestamp", None)
	event_name=doc.get("event_name", None)

	# add all event properties to a list
	current_event=[]
	current_event.append(event_id)
	current_event.append(event_timestamp)
	current_event.append(event_name)

	# add the event to the final list of events
	all_events.append(current_event)


export_file="export_file_mongo.csv"

with open(export_file, 'w') as fp:
	csvw=csv.writer(fp, delimiter='|')
	csvw.writerows(all_events)


fp.close()


# Uploading csv to S3 bucket
# get credentials
#parser=configparser.ConfigParser()
#parser.read('pipeline.conf')
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3=boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file=export_file

s3.upload_file(export_file, bucket_name, s3_file)