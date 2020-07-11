package com.steckytech.maxwell

import java.net.URISyntaxException
import java.rmi.ServerException
import java.sql.SQLException

import com.djdch.log4j.StaticShutdownCallbackRegistry
import com.zendesk.maxwell.{Maxwell, MaxwellConfig, MaxwellContext, MaxwellWithContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer

class Metadata() {
  val timestamp:Long = System.currentTimeMillis()
}

object MaxwellReceiver {
  //val LOG:Logger = LoggerFactory.getLogger(MaxwellSparkDriver.getClass)
}

// class MaxwellReceiver(q:BlockingQueue[String]) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
class MaxwellReceiver(args:Array[String]) extends Receiver[String](StorageLevel.MEMORY_ONLY) with Serializable {


  //val LOG = MaxwellReceiver.LOG

  var maxwell:(Maxwell,Thread) = (null,null)


  def onStart() {
    println("Startup")

    maxwell = bootMaxwell(this, args)
    maxwell._2.start()

    //LOG.info("Receiver ON START")
    // Setup stuff (start threads, open sockets, etc.) to start receiving data.
    // Must start new thread to receive data, as onStart() must be non-blocking.

    // Call store(...) in those threads to store received data into Spark's memory.

    // Call stop(...), restart(...) or reportError(...) on any thread based on how
    // different errors need to be handled.

    // See corresponding method documentation for more details


    // Start the thread that receives data over a connection


  }

  def onStop() {
    // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
    //if (this.maxwell._2 != null) {
      //this.maxwell._2.join()
    //}
    maxwell._1.terminate()
    maxwell._2.join()
  }


  private def bootMaxwell(receiver:MaxwellReceiver, args:Array[String]): (Maxwell,Thread) = {

    try {
      //Logging.setupLogBridging();
      val config:MaxwellConfig = new MaxwellConfig(args)
      config.producerFactory = new JSONProducerFactory(receiver)

      val context:MaxwellContext = new MaxwellContext(config);

      val maxwell = new MaxwellWithContext(context)

      val thread = new Thread(maxwell)

      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run():Unit = {
          maxwell.terminate();
          StaticShutdownCallbackRegistry.invoke();
        }
      })

      return (maxwell,thread)

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

    (null,null)

  }
}
