import psycopg2
import configparser


parser=configparser.ConfigParser()
parser.read('pipeline.conf')
host = parser.get("postgres_config", "hostname")
port = parser.get("postgres_config", "port")
user = parser.get("postgres_config", "username")
dbname = parser.get("postgres_config", "database")
password = parser.get("postgres_config", "password")



# connect to cluster
conn=psycopg2.connect(
	"dbname="+dbname+
	" user="+user
	+" password="+password
	+" host="+host
	+" port="+port)

m_cursor=conn.cursor()


## create table
#m_query='''create table Orders (OrderId int, OrderStatus varchar(30), LastUpdated timestamp);'''
#m_cursor.execute(m_query)
#conn.commit()


## delete if necessary
#m_query='''truncate table Orders;'''
#m_cursor.execute(m_query)
#conn.commit()


# insert values
a='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Backordered', '2020-06-01 12:00:00');'''
b='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Shipped', '2020-06-09 12:00:25');'''
c='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(2,'Shipped', '2020-07-11 3:05:00');'''
d='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(1,'Shipped', '2020-06-09 11:50:00');'''
e='''INSERT INTO Orders (OrderId, OrderStatus, LastUpdated) VALUES(3,'Shipped', '2020-07-12 12:00:00');'''

for i in [a,b,c,d,e]:
	m_cursor.execute(i)
	conn.commit()



m_cursor.execute("""select * from Orders;""")

results=m_cursor.fetchall()
print(results)


m_cursor.close()
conn.close()

