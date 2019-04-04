package com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp


import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config,ConfigFactory}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.AppConfig
import java.io.File
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.IncomingTransactionDetails
import org.apache.log4j.Logger
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.parser.IncomingCreditTransactionParser
import org.apache.spark.streaming.Minutes


/**
 * Oluwaseyi Otun 
 * 
 */




object SparkSpeedLayerTransactonAggregator{
  
  def main(args : Array[String]){
   
      val bigdataPath = sys.env("BIGDATA_APP_HOME")
       
     val configFile = new File(bigdataPath + "/lamdba.conf")
       
     val configFactory = ConfigFactory.parseFile(configFile).getConfig("appConfig")
     
     val appConfig = AppConfig(configFactory.getString("kafka.host"),configFactory.getString("kafka.topic"),configFactory.getString("kafka.kafkaConsumerGrp"),configFactory.getLong("spark.windowLenght"),configFactory.getLong("spark.slideInterval"),configFactory.getString("spark.checkPointDir"),configFactory.getString("cassandra.username"),
     
     configFactory.getString("cassandra.host"),configFactory.getString("cassandra.password"),configFactory.getString("cassandra.port"),configFactory.getString("cassandra.keyspace"),configFactory.getString("cassandra.table"))
     
     val sparkconf = new SparkConf().setMaster("local[2]").setAppName("SparkSpeedLayerTransactonAggregator")
                     .set("spark.cassandra.connection.host", "127.0.0.1");
                                     
     
     val streamingContext = new StreamingContext(sparkconf, Seconds(5))
      
      
     val kafkaParams = Map[String, Object]("bootstrap.servers" -> appConfig.kafkaBrokerAddress,
                    	"key.deserializer" -> classOf[StringDeserializer],
                    	"value.deserializer" -> classOf[StringDeserializer],
                    	"group.id" -> appConfig.kafkaConsumerGroupid,
                    	"auto.offset.reset" -> "latest",
                    	"enable.auto.commit" -> (false: java.lang.Boolean)
                       )

     val topics: Set[String] = appConfig.kafkatopic.split(",").map(_.trim).toSet
     
     val creditCardTranxMessageStream = KafkaUtils.createDirectStream[String, String](streamingContext,PreferConsistent, Subscribe[String, String](topics, kafkaParams))
     
     val creditCardTranxDstream =  creditCardTranxMessageStream.map(creditCardTranx => IncomingCreditTransactionParser.parseIncomingCreditCardTxMessage(creditCardTranx.value()))
     
    
     
     
     val transactionAggregator = new TransactionAggregator()
      
     
     transactionAggregator.saveTransactionDetailsToCassandra(creditCardTranxDstream, appConfig)
      
    
     
     val transactionAggRollupHourly = creditCardTranxDstream.window(Minutes(1), Seconds(60))
     
     transactionAggregator.saveHourlyRollupTransactionToCassandra(transactionAggRollupHourly, appConfig)
    
     streamingContext.start();
     
     streamingContext.awaitTermination();
    
     streamingContext.stop(stopSparkContext = true, stopGracefully = true)
  }

}


    
  

