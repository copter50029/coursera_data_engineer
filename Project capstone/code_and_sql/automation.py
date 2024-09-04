# Import libraries required for connecting to mysql

# Import libraries required for connecting to DB2 or PostgreSql

# Connect to MySQL
import mysql.connector
connection = mysql.connector.connect(user='root', password='kFDvOxfbbLTzAdRsgHCpHFFl',host='172.21.73.185',database='sales')
# create cursor
cursor = connection.cursor()

# Connect to PostgreSql
import psycopg2
dsn_hostname = '172.21.192.160'
dsn_user='postgres'        # e.g. "abc12345"
dsn_pwd ='gzQ4pZ4MaHaZFVlJJIJzusGN'      # e.g. "7dBZ3wWt9XN6$o0J"
dsn_port ="5432"                # e.g. "50000" 
dsn_database ="sales_database"           # i.e. "BLUDB"
# create connection
conn = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)
cursor_pqs = conn.cursor()

# Function to get the last row_id
def get_last_rowid():
    SQL = "SELECT MAX(ROWID) FROM sales_data"
    cursor.execute(SQL)
    row = cursor.fetchone()
    return row

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.

def get_latest_records(rowid):
    SQL = "SELECT * FROM sales_data WHERE rowid = (SELECT MAX(rowid) FROM sales_data);"
    cursor.execute(SQL)
    rowid = cursor.fetchone()
    return rowid
	

new_records = get_latest_records(last_row_id)

print("New rows on staging datawarehouse = ", new_records)

# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.

def insert_records(records):
     SQL = "INSERT INTO products (product_id, product_name, product_type, product_price) VALUES (%s, %s, %s, %s)"
     cursor_pqs.execute(SQL, records)
     conn.commit()
     return cursor_pqs.rowcount
    
print("New rows inserted into production datawarehouse = ", new_records)

# disconnect from mysql warehouse

conn.close()
# disconnect from DB2 or PostgreSql data warehouse 
cursor_pqs.close()
# End of program