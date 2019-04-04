

HDFS
=====================================================================================================
cd $HADOOP_HOME
bin/start-all.sh
hdfs dfs -mkdir /bigdata/fintech/lambda/datalake/creditcardTxData




Kafka 
=======================================================================================================
cd $Flume_HOME
bin/kafka-topics --create  --zookeeper localhost:2181  --partitions 1 --replication-factor 1 --topic creditCardTxTopic 


Flume
=====================================================================================================
cd $Flume_HOME
bin/flume-ng agent -n lambda -c conf -f conf/lambda_hdfs_flume_conf.properties


  
  
  
Spark
==========================================================================================================  
spark-submit --class com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.SparkSpeedLayerTransactonAggregator --master local[2]  lambda-speed-jar-with-dependencies.jar

spark-submit --class com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.SparkBatchLayerTransactonAggregator --master local[2]  lambda-batch-jar-with-dependencies.jar

 