package com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.util


import org.apache.spark.sql.functions._
import com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.model.BucketModel._
import org.apache.spark.sql.Column



object DataFrameUtils {

  
  
  
  def yearCol(fromField: String): Column = {
    year(col(fromField)) as "year"
  }

  def monthCol(fromField: String): Column = {
    month(col(fromField)) as "month"
  }

  def dayCol(fromField: String): Column = {
    dayofmonth(col(fromField)) as "day"
  }

  def hourCol(fromField: String): Column = {
    hour(col(fromField)) as "hour"
  }

  def minuteCol(fromField: String): Column = {
    minute(col(fromField)) as "minute"
  }
  
}