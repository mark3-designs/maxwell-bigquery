package com.steckytech.maxwell.spark

import java.net.URISyntaxException
import java.rmi.ServerException
import java.sql.SQLException

import com.djdch.log4j.StaticShutdownCallbackRegistry
import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.zendesk.maxwell.producer.ProducerFactory
import com.zendesk.maxwell.{Maxwell, MaxwellConfig, MaxwellContext, MaxwellWithContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.slf4j.LoggerFactory


abstract class MaxwellReceiver[T](configFactory: MaxwellConfigFactory, storage:StorageLevel) extends Receiver[T](storage) with Serializable {

  var maxwell:(Maxwell,Thread,Thread) = (null,null,null)


  def onStart() {
    // onStart() must be non-blocking
    maxwell = bootMaxwell(configFactory)

  }

  def onStop() {
    maxwell._1.terminate()
    maxwell._2.join()
  }

  def getProducerFactory(): ProducerFactory

  private def bootMaxwell(configFactory: MaxwellConfigFactory): (Maxwell,Thread, Thread) = {
    val LOG = LoggerFactory.getLogger(this.getClass)
    val receiver = this

    try {
      // construct the maxwell configuration object inside the receiver thread
      val config:MaxwellConfig = configFactory.build()

      // set maxwell's configuration to our factory that has a reference to this receiver
      // the maxwell producer will call `.store(data)` on this receiver
      config.producerFactory = getProducerFactory()

      val context:MaxwellContext = new MaxwellContext(config);

      val maxwell = new MaxwellWithContext(context)

      val thread = new Thread(maxwell)

      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run():Unit = {
          maxwell.terminate();
          StaticShutdownCallbackRegistry.invoke();
        }
      })

      /*
      val monitor: Thread = new Thread() {
        override def run(): Unit = {
          while (isStarted()) {
            Thread.sleep(3000)
            if (!thread.isAlive) {
              maxwell.terminate()
              receiver.stop("maxwell thread died")
            }
          }
        }
      }

       */

      thread.start()
      //monitor.start()
      return (maxwell,thread,null)

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

    (null,null,null)

  }
}
