from datetime import timedelta
from traceback import extract_tb
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'jvm',
    'start_date': days_ago(0),
    'email': ['jvm@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='process web logs',
    schedule_interval=timedelta(days=1),
)

extract_data = BashOperator(
    task_id='extract_data',
    bash_command='grep -o ' + '\'[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\'' 
    + ' /home/jvm/airflow/capstone/accesslog.txt > /home/jvm/airflow/dags/capstone/extracted-data.txt',
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command='grep -v ' + '\'^198\.46\.149\.143\'' + ' /home/jvm/airflow/dags/capstone/extracted-data.txt > /home/jvm/airflow/dags/capstone/transformed-data.csv',
    dag=dag,
)

load_data = BashOperator(
    task_id='load_data',
    bash_command='tar -czvf /home/jvm/airflow/dags/capstone/weblog.tar.gz /home/jvm/airflow/dags/capstone/transformed-data.csv' ,
    dag=dag,
)

# task pipeline
extract_data >> transform_data >> load_data