#!/bin/bash

sudo nano  /etc/systemd/system/airflow-scheduler.service
sudo nano  /etc/systemd/system/airflow-webserver.service

--------------------------------
sudo mkdir -p /home/project/airflow/dags/finalassignment/staging
sudo mkdir -p /home/jvm/airflow/dags/finalassignment/staging

#Download the dataset from the source to the destination mentioned below using wget command.

#Destination : /home/project/airflow/dags/finalassignment
#	          /home/jvm/airflow/dags/finalassignment

sudo wget  'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz' 

sudo chmod 777 tolldata.tgz 
sudo chmod -R 777  /home/jvm/airflow/dags/finalassignment


cd /home/project/airflow/dags/finalassignment/staging
cd /home/jvm/airflow/dags/finalassignment/staging


mv ETL_toll_data.py /home/jvm/airflow/dags
