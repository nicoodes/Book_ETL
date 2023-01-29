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
#m_cursor.execute('''select version()''')

#m_cursor.execute('''drop database BOOK''')
#m_cursor.execute('''create database BOOK''')
m_cursor.execute('''use BOOK''')

#m_cursor.execute('''create table Orders (OrderId int, OrderStatus varchar(30), LastUpdated timestamp);''')
a='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Backordered', '2020-06-01 12:00:00');'''
b='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Shipped', '2020-06-09 12:00:25');'''
c='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(2,'Shipped', '2020-07-11 3:05:00');'''
d='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Shipped', '2020-06-09 11:50:00');'''

#for i in [a,b,c,d]:
#	m_cursor.execute(i)
#	conn.commit()
#

#m_cursor.execute('''show tables''')
#m_cursor.execute('''describe Orders''')


# Extraction
m_cursor.execute('''select * from  Orders;''')


results=m_cursor.fetchall()
print(results)


m_cursor.close()
conn.close()

