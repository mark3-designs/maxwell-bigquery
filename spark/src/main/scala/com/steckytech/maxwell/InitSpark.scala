package com.steckytech.maxwell

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

trait InitSpark {

  val session = SparkSession.builder()
    .appName("maxwell")
    .master("local[4]")    // url of spark server
    .getOrCreate()

  /*
    val sqlContext = session.sqlContext
   */
  val stream = new StreamingContext(session.sparkContext, Seconds(1))

  /* cruft left-over:
  def reader = spark.read
      .option("header",true)
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
    def readerWithoutHeader = spark.read
      .option("header",true)
      .option("inferSchema", true)
      .option("mode", "DROPMALFORMED")
   */


    private def init = {
      session.sparkContext.setLogLevel("ERROR")
      Logger.getLogger("org").setLevel(Level.ERROR)
      Logger.getLogger("akka").setLevel(Level.ERROR)
      LogManager.getRootLogger.setLevel(Level.ERROR)
    }
    init

    def close = {
      session.close()
    }

}
