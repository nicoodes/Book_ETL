from pymongo import MongoClient
import datetime
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

# connect to db where colelction resides
mongo_db=mongo_client[database_name]

mongo_collection=mongo_db[collection_name]

print(mongo_db)
print(mongo_collection)

event_1={
	"event_id":1,
	"event_timestamp":datetime.datetime.today(),
	"event_name":"signup"
}

event_2={
	"event_id":2,
	"event_timestamp":datetime.datetime.today(),
	"event_name":"pageview"
}

event_3={
	"event_id":3,
	"event_timestamp":datetime.datetime.today(),
	"event_name":"login"
}


#insert the 3 documents
#mongo_collection.insert_one(event_1)
#mongo_collection.insert_one(event_2)
#mongo_collection.insert_one(event_3)