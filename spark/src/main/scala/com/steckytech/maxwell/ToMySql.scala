package com.steckytech.maxwell

import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.steckytech.maxwell.spark.JSONReceiver
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import org.slf4j.LoggerFactory
import com.steckytech.maxwell.spark.JdbcHelper._


object ToMySql {

  val LOGGER = LoggerFactory.getLogger(ToMySql.getClass)


  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting Maxwell Stream...");

    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      .master("local[4]")
      .getOrCreate()

    val destination = "binlog-stream"

    val batchDuration = Seconds(30)

    val streamingContext = new StreamingContext(session.sparkContext, batchDuration)

    // create maxwell receiver
    val receiver = new JSONReceiver(new MaxwellConfigFactory(args))

    // create stream
    val stream = streamingContext.receiverStream(receiver)

    // write to hdfs/file output
    // stream.saveAsTextFiles(destination, "json")
    stream.foreachRDD((data, time) =>

      session.createDataset( session.sparkContext.makeRDD(data.collect()) )(Encoders.STRING)
        .write
        .mode(SaveMode.Append)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("database", "stocks")
        .jdbc("jdbc:mysql://mysql.cyberdyne:3306/"+ "stocks" +"?serverTimezone=US/Mountain&autoReconnect=true",
          "stocks" +".cdc",
          dbConnectionProperties(dbUser,dbPass)
      )

    )

    streamingContext.start()

    streamingContext.awaitTermination()

  }


}

