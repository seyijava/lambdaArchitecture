package com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp


import java.io.File
import org.apache.spark.sql.SparkSession
import com.typesafe.config.{Config,ConfigFactory}
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
import org.apache.spark.sql.SQLContext
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.util.Utils.measureExecutionTime
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

case class AppConfig(hdfsurl: String,datalakeInputDir: String, datalakeOutParqetDir: String, cassandraHost: String, cassandraKeyspace: String)


/**
 * Otun Oluwaseyi
 */
object SparkBatchLayerTransactonAggregator {

  
  
  def main(args : Array[String]){
   
      val bigdataPath = sys.env("BIGDATA_APP_HOME")
       
     val configFile = new File(bigdataPath + "/lamdba_batch.conf")
       
     val configFactory = ConfigFactory.parseFile(configFile).getConfig("appConfig")
     
     val appConfig = AppConfig(configFactory.getString("hdfs.url"),configFactory.getString("hdfs.datalakeInputDir"),configFactory.getString("hdfs.datalakeOutputDir"),configFactory.getString("cassandra.host"),configFactory.getString("cassandra.keyspace"))
     
     val sparkconf = new SparkConf().setMaster("local[2]").setAppName("SparkBatchLayerTransactonAggregator")
                     .set("spark.cassandra.connection.host",appConfig.cassandraHost)
                     .set("spark.sql.shuffle.partitions", "1") 
      
     val sparkContext = new  SparkContext(sparkconf)
      
     val  sqlContext = new SQLContext(sparkContext) 
     
   measureExecutionTime{
     DataPreProcessingParquet.preProcessData(sqlContext, appConfig.hdfsurl , appConfig.datalakeInputDir , appConfig.datalakeOutParqetDir , false)
     }
     
     measureExecutionTime{
     BatchViewDataProcessor.processBatchView(sqlContext, appConfig.datalakeOutParqetDir, appConfig)
     deleteParqueDir(appConfig.datalakeOutParqetDir, appConfig.hdfsurl )
      }
     
     
  
     
     System.exit(0)
                                     
  }
  
  
   def deleteParqueDir(outDirPath: String,url: String){
       val config = new Configuration();
    	config.set("fs.default.name", url);
       val fs = FileSystem.get( config )
       if (fs.exists(new Path(outDirPath)))
       fs.delete(new Path(outDirPath), true)
   }
   
}