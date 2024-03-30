import sqlite3
import pandas as pd
conn = sqlite3.connect('STAFF.db')
table_name = 'Departments'
attribute_list = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']
file_path = 'Departments.csv'
df = pd.read_csv(file_path, names = attribute_list)
df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
# Query 2: Display only the FNAME column for the full table.
query_statement = f"SELECT DEP_NAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
# Query 3: Display the count of the total number of rows.
query_statement = f"SELECT MANAGER_ID FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)
data_dict = {'DEPT_ID' : [100],
            'DEP_NAME' : ['John'],
            'MANAGER_ID' : ['Doe'],
            'LOC_ID' : ['Paris'],}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name, conn, if_exists = 'append', index =False)
print('Data appended successfully') 

conn.close()