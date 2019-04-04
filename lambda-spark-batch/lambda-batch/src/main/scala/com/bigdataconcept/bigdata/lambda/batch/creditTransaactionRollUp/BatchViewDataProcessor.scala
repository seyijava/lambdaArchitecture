package com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import java.util.GregorianCalendar
import java.sql.Timestamp
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import java.util.UUID;
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max}
import org.apache.spark.sql.SaveMode
import java.util.Calendar
import java.io.File
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.AppConfig

object BatchViewDataProcessor {

  
  
  
   def processBatchView(sqlContext: SQLContext,  destinatonUrl: String,config: AppConfig) = {
       
     
         val dataFrame = sqlContext.read.parquet(destinatonUrl)
         val timestamp = new Timestamp(System.currentTimeMillis());
         val aggregateByCardType =   dataFrame.groupBy("cardType","hour").agg(count("cardType"),sum("amount") )
         val aggregateByMerchant =  dataFrame.groupBy("merchant", "hour").agg(count("merchant"),sum("amount") )
         val aggregateByCountry = dataFrame.groupBy("country","hour").agg(count("country"),sum("amount") )
          aggregateByCountry.printSchema
           val calendar = new GregorianCalendar()
           calendar.setTime(timestamp)
          val (year,month,day,hour,min,seconds) = getFullSplitDate(timestamp);
          val id = System.currentTimeMillis();
          
          val hourlyAggregatedByCardTypeTrnx = aggregateByCardType
                          .withColumn("id", lit(id))
                          .withColumnRenamed("cardType", "cardtype")
                          .withColumnRenamed("count(cardType)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                          .withColumn("trxntimestamp",lit(timestamp))
          
          hourlyAggregatedByCardTypeTrnx.schema.printTreeString
          hourlyAggregatedByCardTypeTrnx.show
          
          
          
          val hourlyAggregatedByMerchantTrnx = aggregateByMerchant
                          .withColumn("id", lit(id))
                          .withColumnRenamed("count(merchant)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                          .withColumn("trxntimestamp",lit(timestamp))
          
          hourlyAggregatedByMerchantTrnx.schema.printTreeString
          hourlyAggregatedByMerchantTrnx.show
          
          
            val hourlyAggregatedByCountryTrnx = aggregateByCountry
                           .withColumn("id", lit(id))
                          .withColumnRenamed("count(country)", "totalcount")
                          .withColumnRenamed("sum(amount)", "totalsum")
                          .withColumn("hour", lit(hour))
                          .withColumn("day", lit(day))
                          .withColumn("min", lit(min))
                          .withColumn("month", lit(month))
                          .withColumn("year",lit(year))
                          .withColumn("trxntimestamp",lit(timestamp))
                          
                          
                          
          hourlyAggregatedByCountryTrnx.schema.printTreeString
          hourlyAggregatedByCountryTrnx.show
          
      
          hourlyAggregatedByCardTypeTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "BatchHourlyTransactionRollUpPerCardType", "keyspace" -> config.cassandraKeyspace, "cluster" -> config.cassandraHost)).save()
          hourlyAggregatedByMerchantTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "BatchHourlyTransactionRollUpPerMerchant", "keyspace" -> config.cassandraKeyspace, "cluster" -> config.cassandraHost)).save()
          hourlyAggregatedByCountryTrnx.write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Append).options(Map("table" -> "BatchHourlyTransactionRollUpPerCountry", "keyspace" -> config.cassandraKeyspace, "cluster" -> config.cassandraHost)).save()
      
    
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