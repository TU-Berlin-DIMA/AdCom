#!/bin/bash
#set -x
#set -v

#######################################################################
## some variables
#######################################################################
bold=$(tput bold)
normal=$(tput sgr0)
green=$(tput setaf 2)
#######################################################################

echo "Launching the data source application:"
echo "java -classpath /home/flink/flink-1.9.0-partition/lib/flink-dist_2.11-1.10.jar:../MqttDataProducer.jar org.apache.flink.streaming.examples.utils.MqttDataProducer -input /home/felipe/Temp/1524-0.txt"
echo "To consume data on the terminal use:"
echo "mosquitto_sub -h 127.0.0.1 -p 1883 -t topic-data-source"
echo "To change the frequency of emission use:"
echo "mosquitto_pub -h 127.0.0.1 -p 1883 -t topic-frequency-data-source -m 'miliseconds'"
echo
echo "Lauching the flink pre-aggregate application"
echo "./bin/flink run ../WordCountPreAggregate.jar -pre-aggregate-window 0 -input mqtt -output mqtt &"
echo "To consume data from the flink application"
echo "mosquitto_sub -h 127.0.0.1 -t topic-data-sink"
echo



