#!/usr/bin/env bash

########################################################################
# LOCAL ENVIRONMENT
########################################################################
# Flink libraries
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-csv-1.12-SNAPSHOT.jar lib/flink-csv-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-dist_2.12-1.12-SNAPSHOT.jar lib/flink-dist_2.12-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-json-1.12-SNAPSHOT.jar lib/flink-json-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-shaded-zookeeper-3.4.14.jar lib/flink-shaded-zookeeper-3.4.14.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-table-blink_2.12-1.12-SNAPSHOT.jar lib/flink-table-blink_2.12-1.11.2.jar
cp /home/felipe/workspace-idea/flink-partition-tests/flink-dist/target/flink-1.12-SNAPSHOT-bin/flink-1.12-SNAPSHOT/lib/flink-table_2.12-1.12-SNAPSHOT.jar lib/flink-table_2.12-1.11.2.jar

# Flink applications
cp /home/felipe/workspace-idea/flink-partition-tests/flink-examples/flink-examples-streaming/target/flink-examples-streaming_2.12-1.12-SNAPSHOT-WordCount.jar ../flink-applications/
# Flink using AdCom
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar ../flink-applications/
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountDistinctPreAggregate.jar ../flink-applications/
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideAvgPassengerPreAggregate.jar ../flink-applications/
# Flink using Table API
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar ../flink-applications/
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountDistinctTablePreAggregate.jar ../flink-applications/
cp /home/felipe/workspace-idea/flink-partition-tests/flink-adcom/target/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideAvgPassengerTablePreAggregate.jar ../flink-applications/

# Executing Flink applications
# ./bin/flink run ../flink-applications/flink-examples-streaming_2.12-1.12-SNAPSHOT-WordCount.jar --input /home/felipe/Temp/1524-0-4.txt

# AdCom: TaxiRide pre-aggregation
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller false -pre-aggregate-window-timeout 2000 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountDistinctPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -disableOperatorChaining true -input /home/flink/nycTaxiRides.gz -input-par true -output mqtt -sinkHost 127.0.0.1

# Table API: TaxiRide pre-aggregation
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar -input /home/flink/nycTaxiRides.gz -output mqtt -sinkHost 192.168.56.101 -disableOperatorChaining false -mini_batch_enabled true -mini_batch_latency 1_s -mini_batch_size 1000 -mini_batch_two_phase true -parallelism-table 8

########################################################################
# CLUSTER ENVIRONMENT
########################################################################
# AdCom: scaling parallel instance of the pre-agg
# 8  reducers
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining true -slotSplit 1 -parallelism-group-02 8 -output mqtt -sinkHost XXX.XXX.XXX.XXX
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountDistinctPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining true -slotSplit 1 -parallelism-group-02 8 -output mqtt -sinkHost XXX.XXX.XXX.XXX
# 16 reducers
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -slotSplit 1 -parallelism-group-02 16 -output mqtt -sinkHost XXX.XXX.XXX.XXX
# 24 reducers
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountPreAggregate.jar -controller true -pre-aggregate-window-timeout 500 -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -slotSplit 1 -parallelism-group-02 24 -output mqtt -sinkHost XXX.XXX.XXX.XXX

# Table API: TaxiRide pre-aggregation: 3 sec, 5000 records
# 8  reducers
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining false -parallelism-table 8 -output mqtt -sinkHost XXX.XXX.XXX.XXX -mini_batch_enabled true -mini_batch_latency 3_s -mini_batch_size 5000 -mini_batch_two_phase true
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountDistinctTablePreAggregate.jar -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining false -parallelism-table 8 -output mqtt -sinkHost XXX.XXX.XXX.XXX -mini_batch_enabled true -mini_batch_latency 1_s -mini_batch_size 500 -mini_batch_two_phase true -distinct_agg_split_enabled true
# 16 reducers
# ./bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining false -parallelism-table 16 -output mqtt -sinkHost XXX.XXX.XXX.XXX -mini_batch_enabled true -mini_batch_latency 3_s -mini_batch_size 5000 -mini_batch_two_phase true
# 24 reducers
# /bin/flink run ../flink-applications/flink-adcom_2.12-1.12-SNAPSHOT-TaxiRideCountTablePreAggregate.jar -input /home/flink/flink-applications/nycTaxiRides.gz -input-par true -disableOperatorChaining false -parallelism-table 24 -output mqtt -sinkHost XXX.XXX.XXX.XXX -mini_batch_enabled true -mini_batch_latency 3_s -mini_batch_size 5000 -mini_batch_two_phase true


########################################################################
# Workload configuration
########################################################################
# echo "/home/flink/flink-applications/nycTaxiRides.gz" > /tmp/workloadFile.txt
# echo "/home/flink/flink-applications/nycTaxiRidesSkew30.gz" > /tmp/workloadFile.txt
# scp /tmp/workloadFile.txt  worker01:/tmp/workloadFile.txt && scp /tmp/workloadFile.txt worker02:/tmp/workloadFile.txt && scp /tmp/workloadFile.txt worker03:/tmp/workloadFile.txt


########################################################################
# Data rate configuration
########################################################################
# Changing the data rate for 1 data source
# echo '1000000000' > /tmp/datarate.txt    # 1    rec/sec
# echo '10000000' > /tmp/datarate.txt      # 100  rec/sec
# echo '2000000' > /tmp/datarate.txt       # 500  rec/sec
# echo '1000000' > /tmp/datarate.txt       # 1K   rec/sec
# echo '200000' > /tmp/datarate.txt        # 5K   rec/sec
# echo '100000' > /tmp/datarate.txt        # 10K  rec/sec
# echo '66666' > /tmp/datarate.txt         # 15K  rec/sec
# echo '50000' > /tmp/datarate.txt         # 20K  rec/sec
# echo '20000' > /tmp/datarate.txt         # 50K  rec/sec
# echo '10000' > /tmp/datarate.txt         # 100K rec/sec
# echo '5000' > /tmp/datarate.txt          # 200K rec/sec
# echo '2000' > /tmp/datarate.txt          # 500K rec/sec
# echo '1000' > /tmp/datarate.txt          # 1M   rec/sec
# echo '500' > /tmp/datarate.txt           # 2M   rec/sec
# Changing the data rate for 8 data source
# echo '1000000000' > /tmp/datarate.txt    # 1    rec/sec
# echo '2000000' > /tmp/datarate.txt       # 500  rec/sec
# echo '1000000' > /tmp/datarate.txt       # 1K   rec/sec
# echo '200000' > /tmp/datarate.txt        # 5K   rec/sec
# echo '800000' > /tmp/datarate.txt        # 1.25K r/s total -> 10K  rec/sec
# echo '66666' > /tmp/datarate.txt         # 15K  rec/sec
# echo '400000' > /tmp/datarate.txt        # 2.5K r/s  total -> 20K  rec/sec
# echo '160000' > /tmp/datarate.txt        # 6.25K r/s total -> 50K  rec/sec
# echo '80000' > /tmp/datarate.txt         # 12.5K r/s total -> 100K rec/sec
# echo '40000' > /tmp/datarate.txt         # 25K r/s   total -> 200K rec/sec
# echo '16000' > /tmp/datarate.txt         # 65.5K r/s total -> 500K rec/sec
# echo '1000' > /tmp/datarate.txt          # 1M   rec/sec
# echo '500' > /tmp/datarate.txt           # 2M   rec/sec

# scp /tmp/datarate.txt worker01:/tmp/datarate.txt && scp /tmp/datarate.txt worker02:/tmp/datarate.txt && scp /tmp/datarate.txt worker03:/tmp/datarate.txt
# scp /tmp/datarate.txt r01:/tmp/datarate.txt && scp /tmp/datarate.txt r02:/tmp/datarate.txt && scp /tmp/datarate.txt r04:/tmp/datarate.txt

