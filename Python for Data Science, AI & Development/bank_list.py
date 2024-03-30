# ETL stands for Extraction-Transformation-Loading process to handle a dataframe
# Import modules/libraries
from bs4 import BeautifulSoup   # Web Scrapping Operation
import requests                 # Server Connection (Type : HTTP/HTTPS)
import pandas as pd             # Dataframe Operation
import numpy as np              # Matrix/Array Operation
import sqlite3                  # MySQL database Operation (Type : SQLite)
from datetime import datetime   # Date and Time Handling Operation
# Define URL for web scrapping
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'

# Define the column headers for csv (dataframe)
table_attribs = ['Name', 'Market Cap (US$ Billion)']

# Define the data save file (JSON format) : Somehow unused
market_cap_file = 'bank_market_cap_1.json'

# Define the data save/load file : Somehow unused
exchange_rate_file = 'exchange_rate.csv'

# Define the end process of ETL endpoint (loading process) into csv file
# : Somehow, it's the same variable as "csv_path"
load_to_file = 'bank_market_cap_gbp.csv'

# One of the most important process for programming is to create operational logs
# Define a log function to record all behaviors of the ETL model
def log_progress(message):
  timestamp_format = '%Y-%h-%d-%H:%M:%S'      # Define a format time stamp for operational record
  time = datetime.now()                       # Use the current time
  timestamp = time.strftime(timestamp_format) # Convert it into the pre-defined format
  with open("code_log.txt", "a") as f:         # Record operational logs as a local .txt file
      f.write(f'{timestamp}, {message}\n')    # With timestamp format, and the input message logs into the file
# The beginning of the ETL model/operation is how you extract the data/information out of the source, and where they're
# Define an extract function to gather the interested data of yours
def extract(url, table_attribs):
  page = requests.get(url).text               # Get the only-text data from the data source via HTTP/HTTPS connection
  data = BeautifulSoup(page, 'html.parser')   # Temporary parse the gathered data into the html.parser tab
  df = pd.DataFrame(columns=table_attribs)    # Create an empty dataframe to receive the gathered data using the pre-defined header as the column headers
  tables = data.find_all('tbody')             # Fine all the "table" content exisiting within the html.parser tab
  # Get each table one by one
  ###===================== Specific Condition/Criterior to filter any unneccessory data out of the gathered data =====================###
  ###===================== You'll need to read the raw data yourselve to get the gist of its structure =====================###
  rows = tables[0].find_all('tr')
  for row in rows:
    if row.find('td') is not None:
      col = row.find_all('td')
      bank_name = col[1].find_all('a')[0]['title']
      market_cap = col[2].contents[0][:-1]
      data_dict = {"Name": bank_name,"MC_USD_Billion": float(market_cap)}
      df1 = pd.DataFrame(data_dict,index=[0])
      df = pd.concat([df,df1], ignore_index = True)
###======================================================================================================================###
  return df   # Return the created dataframe back
# The second step of the ETL model/operation is to transform the stucture of the raw data into any format neccessory for analysis procedure
# Define a data transformation function to convert data format
def transform(df, csv_path):
  # Clean the 'Market Cap (US$ Billion)' column
  exchange_rate = pd.read_csv(csv_path)
  exchange_rate = exchange_rate.set_index("Currency").to_dict()["Rate"]
  df["MC_GBP_Billion"] = [np.round(x*exchange_rate['GBP'],2)for x in df["MC_USD_Billion"]]    
  df["MC_EUR_Billion"] = [np.round(x*exchange_rate['EUR'],2)for x in df["MC_USD_Billion"]]           
  df["MC_INR_Billion"] = [np.round(x*exchange_rate['INR'],2)for x in df["MC_USD_Billion"]]   

  return df   # Return the transformed dataframe back

def load_to_csv(df, csv_path):
  df.to_csv(csv_path, index=False)    # Directly save the dataframe into the input file path (.csv format)

# Define a local database saving function (.db format)
def load_to_db(df, sql_connection, table_name):
  df.to_sql(table_name, sql_connection, if_exists='replace', index=False)   # Directly save the dataframe into the input database path (.db format)
  # (If the file is already exist within the directiory, replace it)
# Define a function to handle MySQL Query
def run_query(query_statement, sql_connection):
  print(query_statement)    # Display the local log (non-saving) into the terminal for MySQL query statement
  query_output = pd.read_sql(query_statement, sql_connection)   # Execute the input query
  print(query_output)       # Display the local log into the terminal for the query result

# Save log for the initializing process of ETL model/operation
log_progress('Preliminaries complete. Initiating ETL process')
# Get the data and assign them into the dataframe variable
df = extract(url, table_attribs)

# Save log for extraction process
log_progress('Data extraction complete. Initiating Transformation process')

# Display the top 5 rows of the dataframe
print(df.head(11))

# Execute the data transformation process
df = transform(df, exchange_rate_file)

# Save log for transformation process
log_progress('Data transformation complete. Initiating loading process')

# Display the transformed dataframe
print(df)
# Execute the load process (into .csv file)
csv_path = 'bank_market_cap_gbp.csv'  # Define a valid path
load_to_csv(df, csv_path)

# Save log for the loading process
log_progress('Data saved to CSV file')
# Execute the load process (into .csv file)
csv_path = 'bank_market_cap_gbp.csv'  # Define a valid path
load_to_csv(df, csv_path)

# Save log for the loading process
log_progress('Data saved to CSV file')
# Create a connection to the local SQLite database
sql_connection = sqlite3.connect('World_bank_networth.db')
table_name = 'banks'  # Define a valid table name

# Save log for MySQL connection
log_progress('SQL Connection initiated.')
# Execute the load process (into .db file)
load_to_db(df, sql_connection, table_name)

# Save log for the loading process
log_progress('Data loaded to Database as table. Running the query')
# Execute query to load the data fromt the local SQLite database
query_statement = f"SELECT * from {table_name} WHERE [MC_USD_Billion] >= 10"
run_query(query_statement, sql_connection)

# Save log for the loading process
log_progress('Process Complete.')

# Terminate the execution of SQLite
#sql_connection.close()