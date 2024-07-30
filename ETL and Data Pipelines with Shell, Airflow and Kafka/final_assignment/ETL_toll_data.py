# Import the libraries
# Import libraries
import csv
import os
import tarfile
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd
#DAG argument
default_args = {
    'owner': 'barry',
    'start_date': datetime.now(),#date today
    'email': ['sudo555@gmail.com'],
    'email_on_failure': True,
    'email_on_retry':True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define DAG

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    
)
# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator

import datetime
import tarfile

#DAG argument
default_args = {
    'owner': 'barry',
    'start_date': datetime.now(),#date today
    'email': ['sudo555@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#define DAG

dag = DAG(
    'ETL_toll_data',
    schedule_interval=timedelta(days=1),
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    
)
#define the unzip function
DESTINATION = "/home/project/airflow/dags/finalassignment"
source = os.path.join(DESTINATION, "tolldata.tgz")
def unzip():
    with tarfile.open(source, 'r:gz') as tar:
        tar.extractall(path=DESTINATION)
    
# Define the task named unzip_data to unzip data
unzip_data = PythonOperator(
    task_id = 'unzip_data',
    python_callable = unzip,
    dag=dag
)

#Define the function extract_data_from_csv #chande photo in document
def extract_data_from_csv(**kwargs):
    #extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file
    vehicle_data = kwargs['extract_field'][0]
    csv_data = kwargs['extract_field'][1]
    df = pd.read_csv(vehicle_data)
    df = df[['Rowid', 'Timestamp', 'Anonymized_Vehicle_ID', 'Vehicle_Type']]
    df.to_csv(csv_data, index=False)
#Define the task named extract_data_from_csv to to extract the fields Rowid, Timestamp, Anonymized Vehicle number, and Vehicle type from the vehicle-data.csv file 
vehicle_data = os.path.join(DESTINATION, "vehicle-data.csv")
csv_data = os.path.join(DESTINATION, "csv_data.csv")

extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable= extract_data_from_csv,
    extract_field = [vehicle_data, csv_data],
    dag=dag
)

#Create task named extract_data_from_tsv to extract the fields Number of axles, Tollplaza id, and Tollplaza code from the tollplaza-data.tsv
def extract_data_from_tsv(**kwargs):
    toll_data = kwargs['extract_field'][0]
    tsv_data = kwargs['extract_field'][0]
    df = pd.read_csv(toll_data)
    df = df[['Number_of_axles','tollplaza_id','Tollplaza_code']]
    df.to_csv(tsv_data, index=False)

toll_data = os.path.join(DESTINATION, "tollplaza-data.tsv")
tsv_data = os.path.join(DESTINATION, "tsv_data.csv")
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable= extract_data_from_tsv,
    extract_field = [toll_data, tsv_data],
    dag=dag
)

#Create a task named extract_data_from_fixed_width to extract the fields Type of Payment code, and Vehicle Code from the fixed width file payment-data.txt and save it into a file named fixed_width_data.csv
def extract_data_from_fixed_width(**kwargs):
    payment_data = kwargs['extract_field'][0]
    fixed_width_data = kwargs['extract_field'][1]
    df = pd.read_fwf(payment_data, widths=[2, 2], names=['Type_of_Payment_Code', 'Vehicle_Code'])
    df.to_csv(fixed_width_data, index=False)

payment_data = os.path.join(DESTINATION,"payment-data.txt ")
fixed_width_data = os.path.join(DESTINATION,"fixed_width_data.csv")

extract_data_from_fixed_width = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable = extract_data_from_fixed_width,
    extract_field = [payment_data,fixed_width_data],
    dag=dag
)
