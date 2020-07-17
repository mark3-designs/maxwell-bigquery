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
      .config("spark.driver.memory", "512g")
      .config("spark.executor.memory", "512m")
      // .master("local[4]")

      .getOrCreate()

    val destination = "binlog-stream"

    val batchDuration = Seconds(30)

    val streamingContext = new StreamingContext(session.sparkContext, batchDuration)

    // create maxwell receiver
    val receiver = new JSONReceiver(new MaxwellConfigFactory(args))

    // create stream
    val stream = streamingContext.receiverStream(receiver)

    // write to hdfs/file output
    stream.saveAsTextFiles(destination, "json")

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}
