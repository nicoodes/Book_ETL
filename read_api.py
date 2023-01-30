import requests, json, csv, configparser, boto3


sample_data={"message": "success",
			"request": {"altitude": 100,
					"datetime": 1596384217,
					"latitude": 42.36,
					"longitude": 71.05,
					"passes": 5},
			"response": [{"duration": 623,
						"risetime": 1596384449
						},{"duration": 169,
						"risetime": 1596390428
						},{"duration": 482,
						"risetime": 1596438949
						},{"duration": 652,
						"risetime": 1596444637
						},{"duration": 624,
						"risetime": 1596450474}]}


#url='http://api.open-notify.org/iss-now.json?lat=42.36&lon=71.05'
#api_response=requests.get(url)
# response_json=json.loads(api_response.content)
#print(api_response.text)
# print(response_json)
lat=42.36
lon=71.05

#print(sample_data)
response_json=sample_data
all_pases=[]

for response in response_json['response']:
	current_pass=[]

	current_pass.append(lat)
	current_pass.append(lon)

	current_pass.append(response['duration'])
	current_pass.append(response['risetime'])

	all_pases.append(current_pass)

print(all_pases)


export_file="export_file_api.csv"

with open(export_file, 'w') as fp:
	csvw=csv.writer(fp, delimiter='|')
	csvw.writerows(all_pases)

fp.close()


# Uploading csv to S3 bucket
# get credentials
parser=configparser.ConfigParser()
parser.read('pipeline.conf')
access_key = parser.get("aws_boto_credentials", "access_key")
secret_key = parser.get("aws_boto_credentials", "secret_key")
bucket_name = parser.get("aws_boto_credentials", "bucket_name")

s3=boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)

s3_file=export_file

s3.upload_file(export_file, bucket_name, s3_file)