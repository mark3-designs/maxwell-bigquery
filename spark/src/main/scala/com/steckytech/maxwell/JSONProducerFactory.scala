package com.steckytech.maxwell

import com.zendesk.maxwell.{MaxwellContext}
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap

class JSONProducerFactory(receiver: MaxwellReceiver) extends com.zendesk.maxwell.producer.ProducerFactory {

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    new JSONProducer(receiver, maxwell)
  }

}

class JSONProducer(receiver: MaxwellReceiver, maxwell:MaxwellContext) extends AbstractProducer(maxwell) {
  override def push(r: RowMap): Unit = {
    val item = r.toJSON(maxwell.getConfig.outputConfig)
    while (!receiver.isStarted()) {
      Thread.sleep(500)
    }
    receiver.store(item)
    maxwell.setPosition(r.getNextPosition)
  }

  /*

  private def bootMaxwell(args:Array[String]): (Maxwell,Thread) = {

    try {
      //Logging.setupLogBridging();
      val config:MaxwellConfig = new MaxwellConfig(args)

      //if ( config.log_level != null ) {
      //  Logging.setLevel(config.log_level)
      //}

      val maxwell = new Maxwell(config);
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

   */
}



