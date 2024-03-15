
#Create topic and producer for processing bank ATM transactions

cd /usr/local/kafka
bin/kafka-topics.sh --create --topic news --bootstrap-server localhost:9092
bin/kafka-console-producer.sh --topic news --bootstrap-server localhost:9092
bin/kafka-console-consumer.sh --topic news --from-beginning --bootstrap-server localhost:9092

cd /tmp/kafka-logs
cd news-0
cat 00000000000000000000.log

#Practice exercises
#Problem:

#Problem:
#Create a new topic named weather.
cd /usr/local/kafka
bin/kafka-topics.sh --create --topic weather --bootstrap-server localhost:9092

#Problem:
#Post messages to the topic weather.
bin/kafka-console-producer.sh --topic weather --bootstrap-server localhost:9092


#Kafka Message Key and Offset

#Create a new topic using --topic argument with the name bankbranch. 
cd /usr/local/kafka

bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bankbranch  --partitions 2

bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic bankbranch

#Create a producer for topic bankbranch

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch 


#You can try to publish the following ATM messages after it to produce the messages:

#{"atmid": 1, "transid": 100}

#{"atmid": 1, "transid": 101}

#{"atmid": 2, "transid": 200}

#{"atmid": 1, "transid": 102}

#{"atmid": 2, "transid": 201}


#Start a new consumer to subscribe topic bankbranch

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --from-beginning

#Then, you should see the new 5 messages we just published,
#They are not consumed in the same order as they published.

#Produce and consume with message keys
#Start a new producer with message key enabled:
#In this step, you will be using message keys to ensure messages with the same key will be consumed with
#the same order as they published

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch --property parse.key=true --property key.separator=:

#Produce the following message with key to be ATM ids

#1:{"atmid": 1, "transid": 102}

#1:{"atmid": 1, "transid": 103}

#2:{"atmid": 2, "transid": 202}

#2:{"atmid": 2, "transid": 203}

#1:{"atmid": 1, "transid": 104}


bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --from-beginning --property print.key=true --property key.separator=:

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group atm-app

bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic bankbranch --property parse.key=true --property key.separator=:

#1:{"atmid": 1, "transid": 105}
#2:{"atmid": 2, "transid": 204}

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group atm-app

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

#Reset offset

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092  --topic bankbranch --group atm-app --reset-offsets --to-earliest --execute

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app

bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092  --topic bankbranch --group atm-app --reset-offsets --shift-by -2 --execute

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic bankbranch --group atm-app