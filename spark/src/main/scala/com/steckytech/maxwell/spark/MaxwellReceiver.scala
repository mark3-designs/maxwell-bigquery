package com.steckytech.maxwell.spark

import java.net.URISyntaxException
import java.rmi.ServerException
import java.sql.SQLException

import com.djdch.log4j.StaticShutdownCallbackRegistry
import com.steckytech.maxwell.Filter
import com.zendesk.maxwell.{Maxwell, MaxwellConfig, MaxwellContext, MaxwellWithContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory

import scala.collection.mutable


class MaxwellReceiver(args:Array[String]) extends Receiver[String](StorageLevel.MEMORY_ONLY_2) with Serializable {



  var maxwell:(Maxwell,Thread) = (null,null)

  var filters:mutable.MutableList[Filter] = mutable.MutableList[Filter]()

  def onStart() {
    /* onStart() must be non-blocking */
    maxwell = bootMaxwell(args)
  }

  def onStop() {
    maxwell._1.terminate()
    maxwell._2.join()
  }


  def withFilter(filter:Filter): MaxwellReceiver = {
    filters += filter
    this
  }


  private def bootMaxwell(args:Array[String]): (Maxwell,Thread) = {
    val LOG = LoggerFactory.getLogger(this.getClass)

    try {
      val config:MaxwellConfig = new MaxwellConfig(args)

      // set maxwell's configuration to our factory that has a reference to this receiver
      // the maxwell producer will call `.store(data)` on this receiver
      config.producerFactory = new JSONProducerFactory(this, filters.toList)

      val context:MaxwellContext = new MaxwellContext(config);

      val maxwell = new MaxwellWithContext(context)

      val thread = new Thread(maxwell)

      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run():Unit = {
          maxwell.terminate();
          StaticShutdownCallbackRegistry.invoke();
        }
      })

      thread.start()
      return (maxwell,thread)

    } catch {
      case e:SQLException =>
        // catch SQLException explicitly because we likely don't care about the stacktrace
        LOG.error("SQLException: " + e.getLocalizedMessage());
        e.printStackTrace()
        System.exit(1);
      case e:URISyntaxException =>
        // catch URISyntaxException explicitly as well to provide more information to the user
        LOG.error("Syntax issue with URI, check for misconfigured host, port, database, or JDBC options (see RFC 2396)");
        LOG.error("URISyntaxException: " + e.getLocalizedMessage());
        e.printStackTrace()
        System.exit(1);
      case e:ServerException =>
        LOG.error("Maxwell couldn't find the requested binlog, exiting...");
        e.printStackTrace()
        System.exit(2);
      case e:Exception =>
        e.printStackTrace();
        System.exit(3);
    }

    (null,null)

  }
}
