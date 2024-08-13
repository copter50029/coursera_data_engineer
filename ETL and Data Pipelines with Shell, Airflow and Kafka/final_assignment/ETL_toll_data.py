# Import libraries
import csv
import os
import tarfile
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import pandas as pd

def unzip_tolldata(source, destination):
    with tarfile.open(source) as tgz:
        tgz.extractall(destination)

def extract_csv_data(inflie, outfile):
    with open(inflie, 'r') as infile, open(outfile, 'w') as writefile:
        for line in infile:
            selected_colums = ",".join(line.strip().split(",")[:4])
            writefile.write(selected_colums + "\n")

def extract_tsv_data(inflie, outfile):
    with open(inflie, 'r') as infile,open(outfile, 'w') as writefile:
        for line in infile:
            selected_colums = ",".join(line.strip().split("\t")[:4])
            writefile.write(selected_colums + "\n")

def extract_fixed_width_data(infile, outfile):
    with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            for line in readfile:
                # Remove extra spaces and split by space
                cleaned_line = " ".join(line.split())

                # Select columns 10 and 11 (0-based index) directly
                selected_columns = cleaned_line.split(" ")[9:11]
                writefile.write(",".join(selected_columns) + "\n")

def consolidate_data_extracted(infile, outfile):
    combined_csv = pd.concat([pd.read_csv(f) for f in infile], axis=1)
    combined_csv.to_csv(outfile, index=False)

def transform_load_data(infile, outfile):
    with open(infile, "r") as readfile, open(outfile, "w") as writefile:
            reader = csv.reader(readfile)
            writer = csv.writer(writefile)

            for row in reader:
                # Modify the fourth field (index 3) and convert to uppercase
                row[3] = row[3].upper()
                writer.writerow(row)
            

#Define Dag arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': 'copter888@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
DESTINATION = "/home/project/airflow/dags/finalassignment"
source = os.path.join(DESTINATION, "tolldata.tgz")
vehicle_data = os.path.join(DESTINATION, "vehicle-data.csv")
tollplaza_data = os.path.join(DESTINATION, "tollplaza-data.tsv")
payment_data = os.path.join(DESTINATION, "payment-data.txt")
csv_data = os.path.join(DESTINATION, "csv_data.csv")
tsv_data = os.path.join(DESTINATION, "tsv_data.csv")
fixed_width_data = os.path.join(DESTINATION, "fixed_width_data.csv")
extracted_data = os.path.join(DESTINATION, "extracted_data.csv")
transformed_data = os.path.join(DESTINATION, "staging/transformed_data.csv")

#Define Dag
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#create tasks name unzip_tolldata
unzip_data = PythonOperator(
    task_id='unzip_tolldata',
    python_callable=unzip_tolldata,
    op_args=[source, DESTINATION],
    dag=dag
)
    

# Create tasks name extract_csv_data
extract_data_from_csv = PythonOperator(
    task_id = 'extract_csv_data',
    python_callable = extract_csv_data,
    op_args = [vehicle_data, csv_data],
    dag = dag
)

#Create tasks name extract_tsv_data
extract_data_from_tsv =PythonOperator(
    task_id = 'extract_tsv_data',
    python_callable = extract_tsv_data,
    op_args = [tollplaza_data,tsv_data],
    dag = dag
)
#Create tasks name extract_fixed_width
extract_fixed_width = PythonOperator(
    task_id = 'extract_fixed_width_data',
    python_callable = extract_fixed_width_data,
    op_args = [payment_data, fixed_width_data],
    dag = dag
)

#Create tasks name consolidate_data_extracted
consolidate_data = PythonOperator(
    task_id = 'consolidate_data_extracted',
    python_callable = consolidate_data_extracted,
    op_args = [[csv_data, tsv_data, fixed_width_data], extracted_data],
    dag = dag
)

#Create tasks name transform_load_data  
transform_data = PythonOperator(
    task_id = 'transform_load_data',
    python_callable = transform_load_data,
    op_args = [extracted_data, transformed_data],
    dag = dag
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_fixed_width >> consolidate_data >> transform_data