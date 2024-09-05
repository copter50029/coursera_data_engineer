# import the libraries
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'copter',
    'start_date': days_ago(0),
    'email': ['copter888@gmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
# define the DAG
dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='process_web_log',
    schedule_interval=timedelta(days=1),
)
# define the tasks
# define the first task
extract = BashOperator(
    task_id='extract',
    bash_command='cut -f1 -d" " $AIRFLOW_HOME/dags/capstone/accesslog.txt > $AIRFLOW_HOME/dags/capstone/extracted_data.txt',
    dag=dag,
)
# define the second task
transform = BashOperator(
    task_id='transform',
    bash_command='grep -vw "198.46.149.143" $AIRFLOW_HOME/dags/capstone/extracted_data.txt > $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
    dag=dag,
)
# define the third task
load = BashOperator(
    task_id='load',
    bash_command='tar -zcvf $AIRFLOW_HOME/dags/capstone/weblog.tar $AIRFLOW_HOME/dags/capstone/transformed_data.txt',
    dag=dag,
)
# task pipeline
extract >> load >> transform