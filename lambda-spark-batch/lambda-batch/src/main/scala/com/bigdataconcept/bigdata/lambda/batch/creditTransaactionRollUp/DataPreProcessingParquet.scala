package com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.model.BucketModel._
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.util.DataFrameUtils._
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.annotation.InterfaceStability
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.util.Utils.measureExecutionTime


/**
 * @author Oluwaseyi Otun 
 * Data PreProcessing using Parquet storage format. Batch Job fetech and analysis from this format
 **/
object DataPreProcessingParquet {

  
   
    def preProcessData(sqlContext: SQLContext, hdfsUrl: String, inputPath: String, outputPath: String, deleteInputFiles: Boolean = false) = {
     
      val dataFrame = getDataFrame(sqlContext,inputPath)
      dataFrame.printSchema
       writeDataFrameParquet(sqlContext, dataFrame, outputPath, Seq("cardType"))
   
     }
  
  
  private def getDataFrame(sqlContext: SQLContext, sourceFilePath: String): DataFrame = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd")
    val folderDate = dateFormatter.format(new Date())
    val fileSeperator = File.separator
    println(sourceFilePath + fileSeperator + folderDate)
    val df = sqlContext.read.json(sourceFilePath)
    val dataFrame =   df.withColumn("year", yearCol("transactionDate"))
      .withColumn("month", monthCol("transactionDate"))
      .withColumn("day", dayCol("transactionDate"))
      .withColumn("hour", hourCol("transactionDate"))
      .withColumn("minute", minuteCol("transactionDate"))
    return dataFrame
  }
  
  private def writeDataFrameParquet(sqlContext: SQLContext, df: DataFrame, outputDir: String, partitionBy: Seq[String]) ={
    df.write.mode(SaveMode.Append).parquet(outputDir)
   }
  
   
}