import pymysql
import csv, boto3, configparser


parser=configparser.ConfigParser()
parser.read('pipeline.conf')
hostname = parser.get("mysql_config", "hostname")
port = parser.get("mysql_config", "port")
username = parser.get("mysql_config", "username")
dbname = parser.get("mysql_config", "database")
password = parser.get("mysql_config", "password")

conn = pymysql.connect(host=hostname,
	user=username,
	password=password,
	#db=dbname,
	port=int(port))
if conn is None:
	print("Error connecting to the MySQL database")
else:
	print("MySQL connection established!")


m_cursor=conn.cursor()

m_cursor.execute('''use BOOK''')

m_cursor.execute('''select variable_value as bin_log_status 
	from  performance_schema.global_variables
	where variable_name='log_bin';''')
results=m_cursor.fetchall()
print(results)

m_cursor.execute('''select variable_value as bin_log_status 
	from  performance_schema.global_variables
	where variable_name='binlog_format';''')

results2=m_cursor.fetchall()
print(results2)

m_cursor.close()
conn.close()

