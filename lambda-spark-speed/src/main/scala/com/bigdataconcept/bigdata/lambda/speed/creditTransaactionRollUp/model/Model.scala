package com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model


import java.sql.Timestamp



case class AppConfig(kafkaBrokerAddress:  String, kafkatopic: String, kafkaConsumerGroupid: String,windowLenght: Long, slideInterval: Long, checkPointDir: String ,
    cassandrauserName: String , cassandraHost: String, cassandraPassword: String,cassandraPort: String,cassandraKeySpace: String,cassandraTable: String)
extends Serializable


  case class IncomingTransactionDetails(transactionId:String,merchant:String,cardNumber:String,cardType:String,status:String,country:String,amount:Double,transactionDate:String) extends Serializable


  case class TransactionDetails(id: Long,cardNumber:String, year: Int, month: Int,day: Int,hour: Int,min: Int,txntimestamp: Timestamp,transactionId: String,merchant:String,country: String,amount: Double,
                         status: String,
                         txndate: String, cardtype: String)
   
  case class HourlyTransactionRollUpPerMerchant(id: Long,merchant:String, year: Int, month: Int,day: Int,hour: Int,min: Int,trxntimestamp: Timestamp,totalsum: Double,totalcount: Int)  
                         
  case class HourlyTransactionRollUpPerCardType(id: Long,cardtype:String, year: Int, month: Int,day: Int,hour: Int,min: Int,trxntimestamp: Timestamp,totalsum: Double,totalcount: Int)  
                         
  case class HourlyTransactionRollUpPerCountry(id: Long,country:String, year: Int, month: Int,day: Int,hour: Int,min: Int,tnxtimestamp: Timestamp,totalsum: Double,tnxcount: Int)  
   