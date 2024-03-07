python3 -m pip install kafka-python
python3 -m pip install mysql-connector-python==8.0.31

cd kafka_2.12-3.5.1
bin/zookeeper-server-start.sh config/zookeeper.properties

cd kafka_2.12-3.5.1
bin/kafka-server-start.sh config/server.properties


mysql --host=127.0.0.1 --port=3306 --user=root --password=…..!

#>> create database tolldata;

#>> use tolldata;

#>> create table livetolldata(timestamp datetime,vehicle_id int,vehicle_type char(15),toll_plaza_id smallint);

#>> exit


sudo systemctl start zookeeper
sudo systemctl start kafka

cd /usr/local/kafka

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic toll
bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

python3 toll_traffic_generator.py

python3 streaming_data_reader.py

sudo mysql --host=127.0.0.1 --port=3306 --user=root --password=x!
#>> USE tolldata;
#>> select * FROM livetolldata LIMIT 10; 
