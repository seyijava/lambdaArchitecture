package com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp

import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.AppConfig
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.IncomingTransactionDetails
import org.apache.spark.streaming.dstream.DStream
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.util.{Calendar, GregorianCalendar}
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.TransactionDetails
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import java.util.UUID;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.SaveMode
/**
 * @author Oluwaseyi Otun 
 * Transaction Aggregration in rolling window 
 */
class TransactionAggregator extends Serializable {

   @transient lazy val logger = Logger.getLogger(getClass.getName)  
  
   def saveTransactionDetailsToCassandra(transactionDStream: DStream[IncomingTransactionDetails], config: AppConfig)
   {
       transactionDStream.foreachRDD(
         rdd=> 
         if(!rdd.isEmpty)
         {
           
             implicit val cansandrConnector = CassandraConnector(rdd.sparkContext.getConf.set("spark.cassandra.connection.host",config.cassandraHost)
                .set("spark.cassandra.auth.username", config.cassandrauserName)            
                .set("spark.cassandra.auth.password", config.cassandraPassword)    
               )
            val  transactionDetailsRDD = rdd.map(tx =>  
                                             {
                                                val txn_time = Timestamp.valueOf(tx.transactionDate)
                                                val timestamp = new Timestamp(System.currentTimeMillis());
                                                val (year,month,day,hour,min,seconds) = getFullSplitDate(timestamp);
                                                val initStatus = tx.status.toInt
                                                val id = System.currentTimeMillis();
                                                val status = if (initStatus < 5) s"REJECTED" else s"APPROVED"
                                                
                                                TransactionDetails(id,tx.cardNumber, year, month, day, hour, min, txn_time,tx.transactionId , tx.merchant, tx.country, tx.amount, status, tx.transactionDate,tx.cardType)
                                             })
                                             
                                           transactionDetailsRDD.saveToCassandra(config.cassandraKeySpace ,"TransactionDetails",SomeColumns("id","cardNumber","year","month","day","hour","min","txntimestamp","transactionId","merchant","country","amount","status","txndate","cardtype"))
                                              logger.trace("Saving Transction History to Cassandara  data warehouse")
              
         }
        ) 
   }
 
   
   
   def saveHourlyRollupTransactionToCassandra(transactionDStream: DStream[IncomingTransactionDetails], config: AppConfig)
   {
          
        transactionDStream.foreachRDD(
         rdd=> 
         if(!rdd.isEmpty)
         {
          
             val spark = SparkSession.builder
            		     .appName("SparkSpeedLayerTransactonAggregator")
            		     .getOrCreate()
            
           import spark.implicits._
           val dataFrame = rdd.toDF;
           val timestamp = new Timestamp(System.currentTimeMillis());
           
          val aggregateByCardType =  dataFrame.groupBy($"cardType").agg(count($"cardType"),sum($"amount") )
           
          val aggregateByMerchant =  dataFrame.groupBy($"merchant").agg(count($"merchant"),sum($"amount") )
          
          val aggregateByCountry =  dataFrame.groupBy($"country").agg(count($"country"),sum($"amount") )
          
           val calendar = new GregorianCalendar()
           calendar.setTime(timestamp)
          val (year,month,day,hour,min,seconds) = getFullSplitDate(timestamp);
          val id = System.currentTimeMillis();
          
          val hourlyAggregatedByCardTypeTrnx = aggregateByCardType
                          .withColumn("trxntimestamp",lit(timestamp))
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                          .withColumn("id", lit(id))
                          .withColumnRenamed("cardType", "cardtype")
                          .withColumnRenamed("count(cardType)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
          
          hourlyAggregatedByCardTypeTrnx.schema.printTreeString
          hourlyAggregatedByCardTypeTrnx.show
          
          
          
          val hourlyAggregatedByMerchantTrnx = aggregateByMerchant
                          .withColumn("trxntimestamp",lit(timestamp))
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                          .withColumn("year",lit(year))
                          .withColumn("id", lit(id))
                          .withColumnRenamed("count(merchant)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
          
          hourlyAggregatedByMerchantTrnx.schema.printTreeString
          hourlyAggregatedByMerchantTrnx.show
          
          
            val hourlyAggregatedByCountryTrnx = aggregateByCountry
                          .withColumn("tnxtimestamp",lit(timestamp))
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                           .withColumn("id", lit(id))
                          .withColumnRenamed("count(country)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
          hourlyAggregatedByCountryTrnx.schema.printTreeString
          hourlyAggregatedByCountryTrnx.show
          
          
          
          hourlyAggregatedByCardTypeTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "HourlyTransactionRollUpPerCardType", "keyspace" -> config.cassandraKeySpace, "cluster" -> config.cassandraHost)).save()
          hourlyAggregatedByMerchantTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "HourlyTransactionRollUpPerMerchant", "keyspace" -> config.cassandraKeySpace, "cluster" -> config.cassandraHost)).save()
          hourlyAggregatedByCountryTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "HourlyTransactionRollUpPerCountry", "keyspace" -> config.cassandraKeySpace, "cluster" -> config.cassandraHost)).save()
    
         })
   }
   
   
   
  
   
   
   def getFullSplitDate(timeStamp: Timestamp) : (Int,Int,Int,Int,Int,Long) = {
          val calendar = new GregorianCalendar()
           calendar.setTime(timeStamp)
          val year = calendar.get(Calendar.YEAR)
          val month = calendar.get(Calendar.MONTH) + 1
          val day = calendar.get(Calendar.DAY_OF_MONTH)
          val hour = calendar.get(Calendar.HOUR)
          val min = calendar.get(Calendar.MINUTE)
          val sec = calendar.get(Calendar.SECOND)
          return (year,month,day,hour,min,sec);
   }
}