package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer

class JSONProducerFactory extends com.zendesk.maxwell.producer.ProducerFactory {

  var sparkThread:Thread= null

  // var receiver:MaxwellReceiver= null

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    val producer = new JSONProducer(maxwell)

    producer
  }

}
