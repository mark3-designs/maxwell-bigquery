package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer

class RDDProducerFactory extends com.zendesk.maxwell.producer.ProducerFactory with InitSpark {

  var sparkThread:Thread= null

  // var receiver:MaxwellReceiver= null

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    val producer = new RDDProducer(maxwell)

    val receiver = new MaxwellReceiver(producer.queue)
    // receiver.init(session.sparkContext, stream, producer.queue)

    // initialize spark stream in a separate thread
    sparkThread= new Thread() {
      override def run():Unit = {

        val cdc = stream.receiverStream(receiver)

        cdc.print(100)

        stream.start()
        stream.awaitTermination()

      }
    }

    sparkThread.start()
    producer
  }

}
