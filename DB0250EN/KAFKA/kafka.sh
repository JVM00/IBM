mysql --host=127.0.0.1 --port=3306 --user=root --password=…..!

#>> create database tolldata;

#>> use tolldata;

#>> create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);

#>> exit

python3 -m pip install kafka-python
python3 -m pip install mysql-connector-python==8.0.31

python3 streaming_data_reader.py
