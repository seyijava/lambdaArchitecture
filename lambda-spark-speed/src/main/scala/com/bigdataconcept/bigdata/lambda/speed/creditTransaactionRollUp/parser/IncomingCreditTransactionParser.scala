package com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.parser



import java.text.SimpleDateFormat;
import org.apache.log4j.Logger
import java.sql.Date;
import com.google.gson.Gson
import com.bigdataconcept.bigdata.lambda.speed.creditTransaactionRollUp.model.IncomingTransactionDetails

object IncomingCreditTransactionParser {

   val log = Logger.getLogger(getClass.getName) 
   
   
     
  def parseIncomingCreditCardTxMessage(message: String) : IncomingTransactionDetails =
  {
      var trxDetail = new Gson().fromJson(message, classOf[IncomingTransactionDetails]);
      return trxDetail
  }
}