package com.steckytech.maxwell

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object SparkRunner {

  val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting up...");


    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      // .config("spark.kryo.registration", com.steckytech.maxwell.kryo.Registrator.getClass.getCanonicalName)
      //.master("spark://hadoop04:7077")    // url of spark server
      //.master("local[2]")    // url of spark server
      .getOrCreate()

    val streamingContext = new StreamingContext(session.sparkContext, Seconds(5))

    val cdc = streamingContext.receiverStream(new MaxwellReceiver(args)); // maxwell.receiver)

    cdc.repartition(4).map(r => "RECORD =======\n"+ r +"\n").print

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}

