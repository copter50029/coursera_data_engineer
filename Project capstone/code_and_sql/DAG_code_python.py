# Import libraries
import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
#DESTINATION Flie
DESTINATION = "/home/project/airflow/dags/"
txt_data = os.path.join(DESTINATION, "accesslog.txt")
weblog_tar = os.path.join(DESTINATION, "weblog.tar")
extracted_data = os.path.join(DESTINATION, "extracted_data.txt")
transformed_data = os.path.join(DESTINATION, "transformed_data.txt")

#define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': 'copter888@gmail.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
#define dag (process_web)
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Apache Airflow capstone project',
    schedule_interval=timedelta(days=1),
)

#dafine task

# Create tasks name extract_txt_data
def extract_txt_data(infile, outfile):
    with open(infile, 'r') as infile, open(outfile, 'w') as writefile:
        for line in infile:
            selected_colums = ",".join(line.strip().split(",")[:4])
            writefile.write(selected_colums + "\n")

extract_data = PythonOperator(
    task_id = 'extract_txt_data',
    python_callable = extract_txt_data,
    op_args = [txt_data,DESTINATION],
    dag = dag
)

#Create task name transformed_data
def filter_ip_address(infile, outfile):
    with open(infile, 'r') as infile:
        lines = infile.readlines()
    
    # Filter out lines containing the specified IP address
    filtered_lines = (line for line in lines if "198.46.149.143" not in line)
    
    with open(outfile, 'w') as outfile:
        outfile.writelines(filtered_lines)

transform_data = PythonOperator(
    task_id = 'transform_load_data',
    python_callable = filter_ip_address,
    op_args = [extracted_data, transformed_data],
    dag = dag
)

#Create task name load_data
def load_tar_data(infile, outfile):
    with open(infile, 'r') as in_file:
        data = in_file.read()
    with open(outfile, 'w') as out_file:
        out_file.write(data)

load_data = PythonOperator(
    task_id = 'load_data',
    python_callable = load_tar_data,
    op_args = [transformed_data, weblog_tar],
    dag = dag
)

extracted_data >> transform_data >> load_data