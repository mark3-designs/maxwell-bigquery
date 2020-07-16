package com.steckytech.maxwell

import com.steckytech.maxwell.spark.MaxwellReceiver
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object SparkRunner {

  val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting up...");


    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "768m")
      .getOrCreate()

    val streamingContext = new StreamingContext(session.sparkContext, Seconds(5))

    val cdc = streamingContext.receiverStream(new MaxwellReceiver(args));

    process(session, cdc).saveAsTextFiles("/user/brad/result", "json")

    streamingContext.start()

    streamingContext.awaitTermination()

  }

  def process(session: SparkSession, stream:ReceiverInputDStream[String]): ReceiverInputDStream[String] = {
    stream.repartition(4).map(r => handl(r))
    stream
  }

  def handl(json:String): Unit = {
  }
}

