package com.steckytech.maxwell.spark

import com.steckytech.maxwell.Filter
import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap

class JSONProducerFactory(receiver: MaxwellReceiver, filters:List[Filter]) extends com.zendesk.maxwell.producer.ProducerFactory {

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    new JSONProducer(receiver, maxwell, filters)
  }

}

class JSONProducer(receiver: MaxwellReceiver, maxwell:MaxwellContext, filter: List[Filter]) extends AbstractProducer(maxwell) {
  override def push(r: RowMap): Unit = {
    val item = r.toJSON(maxwell.getConfig.outputConfig)
    receiver.store(item)
    maxwell.setPosition(r.getNextPosition)
  }

}



