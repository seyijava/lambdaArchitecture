package com.bigdataconcept.bigdata.lambda.batch.creditTransaactionRollUp.util

object Utils {

  
  def measureExecutionTime[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis()
    val ms = (t1 - t0)
    val s = ms / 1000
    println(s"Elapsed time: $ms ms \t ($s seconds)")
    result
  }
}