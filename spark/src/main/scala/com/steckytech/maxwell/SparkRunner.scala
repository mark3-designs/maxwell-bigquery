package com.steckytech.maxwell

import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.steckytech.maxwell.spark.JSONReceiver
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


object SparkRunner {

  val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting Maxwell Stream...");

    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      .master("yarn")
      .getOrCreate()

    val destination = "/user/brad/binlog-stream"

    val batchDuration = Seconds(30)

    val streamingContext = new StreamingContext(session.sparkContext, batchDuration)

    // create maxwell receiver
    val receiver = new JSONReceiver(new MaxwellConfigFactory(args))

    // create stream
    val stream = streamingContext.receiverStream(receiver)

    LOGGER.info("Reading Stream")

    // write to hdfs/file output
    // stream.foreachRDD((data, time) => println(time +" "+ data.collect().toList))

    stream.saveAsTextFiles(destination, "json")

    LOGGER.info("Saving Stream to "+ destination)

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}

