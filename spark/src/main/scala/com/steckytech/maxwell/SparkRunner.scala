package com.steckytech.maxwell

import java.net.URISyntaxException
import java.rmi.ServerException
import java.sql.SQLException

import com.djdch.log4j.StaticShutdownCallbackRegistry
import com.zendesk.maxwell.{Maxwell, MaxwellConfig, MaxwellContext}
import com.zendesk.maxwell.util.Logging
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object SparkRunner {


  //val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  def main(args:Array[String]):Unit = {
    val session = SparkSession.builder()
      .appName("maxwell")
      // .master("spark://hadoop04:7077")    // url of spark server
      // .master("local[2]")    // url of spark server
      .getOrCreate()

    /*
      val sqlContext = session.sqlContext
     */
    val stream = new StreamingContext(session.sparkContext, Seconds(5))

    val maxwell:Maxwell = bootMaxwell(args);
    val q = maxwell.context.getProducer.asInstanceOf[JSONProducer].queue

    //LOGGER.info("Starting up...");
    //LOGGER.info("Maxwell "+ maxwell.getMaxwellVersion)

    val receiver = new MaxwellReceiver(q)
    val cdc = stream.receiverStream(receiver)

    cdc.repartition(4).
      foreachRDD(x => x.foreach(r => println("CDC RECORD =======\n"+ r +"\n")))

    // cdc.saveAsTextFiles("cdc", "txt")

    // cdc.foreachRDD(r => println("ROW: "+ r.collect().foreach(e => println(e))))


    stream.start()

    stream.awaitTermination()
    // maxwell.terminate()

  }

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



  def bootMaxwell(args:Array[String]):Maxwell = {

    try {
      Logging.setupLogBridging();
      val config:MaxwellConfig = new MaxwellConfig(args)

      if ( config.log_level != null ) {
        Logging.setLevel(config.log_level)
      }

      val maxwell:Maxwell = new Maxwell(config);
      val maxwellThread:Thread = new Thread(maxwell)

      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run():Unit = {
          maxwell.terminate();
          StaticShutdownCallbackRegistry.invoke();
        }
      })

      maxwellThread.start();

      return maxwell

    } catch {
      case e:SQLException =>
        // catch SQLException explicitly because we likely don't care about the stacktrace
        //LOGGER.error("SQLException: " + e.getLocalizedMessage());
        e.printStackTrace()
        System.exit(1);
      case e:URISyntaxException =>
        // catch URISyntaxException explicitly as well to provide more information to the user
        //LOGGER.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
        //LOGGER.error("URISyntaxException: " + e.getLocalizedMessage());
        e.printStackTrace()
        System.exit(1);
      case e:ServerException =>
        //LOGGER.error("Maxwell couldn't find the requested binlog, exiting...");
        e.printStackTrace()
        System.exit(2);
      case e:Exception =>
        e.printStackTrace();
        System.exit(3);
    }

    null

  }
}
