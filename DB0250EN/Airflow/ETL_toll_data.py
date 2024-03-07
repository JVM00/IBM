# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#Task 1.1 Define DAG arguments#

default_args = {
    'owner': 'JVM',
    'start_date': days_ago(0),
    'email': ['jvm@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

 #Task 1.2 define the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# define the tasks

#Task 1.3  unzip_data

unzip_data  = BashOperator(
    task_id='unzip_data',
    bash_command='tar --overwrite -xzf /home/jvm/airflow/dags/finalassignment/tolldata.tgz',
    dag=dag,
)


#Task 1.4  define the task extract_data_from_csv

extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -f1-4 -d"," /home/jvm/airflow/dags/finalassignment/vehicle-data.csv > /home/jvm/airflow/dags/finalassignment/csv_data.csv',
    dag=dag,
)

#Task 1.5  define the task extract_data_from_tsv

extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5-7 /home/jvm/airflow/dags/finalassignment/tollplaza-data.tsv | tr "\t" "," | tr -d "\r" > /home/jvm/airflow/dags/finalassignment/tsv_data.csv',
    dag=dag,
)

#Task 1.6  define the task extract_data_from_fixed_width

extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cat /home/jvm/airflow/dags/finalassignment/payment-data.txt | tr -s "[:space:]" | cut -d " " -f 10-11 | tr " " ","  > /home/jvm/airflow/dags/finalassignment/fixed_width_data.csv',
    dag=dag,
)

#Task 1.7  define the task consolidate_data

consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='cd /home/jvm/airflow/dags/finalassignment && paste -d "," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

#Task 1.8  define the task 'transform_data'
create_command = "/home/jvm/airflow/dags/finalassignment/staging/transform_data.sh "
#    bash_command='cd /home/jvm/airflow/dags/finalassignment &&  paste -d "," <(cut -f1-3 -d "," extracted_data.csv) < (cut -f4 -d "," extracted_data.csv | tr "[:lower]" "[:upper:]") <(cut -f5-9 -d "," #extracted_data.csv) > ./staging/transformed_data.csv' ,
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=create_command,
    dag=dag,
)

#Task 1.9  task pipeline

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >>  extract_data_from_fixed_width >> consolidate_data >> transform_data 
