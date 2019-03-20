data_path=/root/xiaoxiang/data/ghcnd/*.csv.gz
kafka_broker_list=cdh7.cloudera.com:9092,cdh3.cloudera.com:9092,cdh4.cloudera.com:9092,cdh5.cloudera.com:9092,cdh6.cloudera.com:9092,cdh8.cloudera.com:9092
kafka_topic=ghcn2

echo "Using $data_path"
echo "Using $kafka_broker_list"
echo "Using $kafka_topic"
echo "Please check your input paramters, wait 10 seconds~"

sleep 10s

cd source
pwd
ls $data_path
which kafka-console-producer

echo "Start send event to kafka"

python data_sender.py --data-path $data_path --sleep-millsecond-per-thousand 100 --enable-null-value --forever | \
    kafka-console-producer --broker-list $kafka_broker_list --topic $kafka_topic