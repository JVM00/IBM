#!/bin/bash
#Transform_data.sh
cd /home/jvm/airflow/dags/finalassignment

 paste -d ',' <(cut -f1-3 -d "," extracted_data.csv) <(cut -f4 -d "," extracted_data.csv | tr "[:lower:]" "[:upper:]") <(cut -f5-9 -d "," extracted_data.csv) > ./staging/transformed_data.csv
