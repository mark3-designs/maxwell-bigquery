package com.steckytech.maxwell.spark

import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap

class JSONProducerFactory(receiver: JSONReceiver) extends com.zendesk.maxwell.producer.ProducerFactory {

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    new JSONProducer(receiver, maxwell)
  }

}

class JSONProducer(receiver: JSONReceiver, maxwell:MaxwellContext) extends AbstractProducer(maxwell) {
  override def push(r: RowMap): Unit = {
    val item = r.toJSON(maxwell.getConfig.outputConfig)
    receiver.store(item)
    maxwell.setPosition(r.getNextPosition)
  }

}



