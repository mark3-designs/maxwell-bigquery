package com.steckytech.maxwell

import java.net.URISyntaxException
import java.rmi.ServerException
import java.sql.SQLException
import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import com.djdch.log4j.StaticShutdownCallbackRegistry
import com.steckytech
import com.zendesk.maxwell.{Maxwell, MaxwellConfig, MaxwellContext, MaxwellWithContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

object SparkRunner {

  val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)


  //val cdcMessages:BlockingQueue[String] = new LinkedBlockingDeque[String]

  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting up...");

    //LOGGER.info("Maxwell "+ maxwell.getMaxwellVersion)

    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      .config("spark.kryo.registration", com.steckytech.maxwell.kryo.Registrator.getClass.getCanonicalName)
      .master("spark://hadoop04:7077")    // url of spark server
      //.master("local[2]")    // url of spark server
      .getOrCreate()

    val streamingContext = new StreamingContext(session.sparkContext, Seconds(5))
    /*
    val cdc = stream.receiverStream(
      new MaxwellReceiver(maxwell.context.getProducer.asInstanceOf[JSONProducer])
    )
    */
    val cdc = streamingContext.receiverStream(new MaxwellReceiver(args)); // maxwell.receiver)

    // cdc.print()

    // cdc.map(x => x)
    cdc.foreachRDD(x => x.foreach(r => println("RECORD =======\n"+ r +"\n")))

    // cdc.saveAsTextFiles("cdc", "txt")

    // cdc.foreachRDD(r => println("ROW: "+ r.collect().foreach(e => println(e))))


    streamingContext.start()

    streamingContext.awaitTermination()
    // maxwell.context.terminate()

  }

}

