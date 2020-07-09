package com.steckytech.maxwell

import java.util.concurrent.{BlockingQueue, LinkedBlockingDeque}

import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap

object RDDProducer {

}

class RDDProducer(maxwell:MaxwellContext) extends AbstractProducer(maxwell)  {

  val queue:BlockingQueue[String] = new LinkedBlockingDeque[String]()

  override def push(r: RowMap): Unit = {
    queue.add(r.toJSON(maxwell.getConfig.outputConfig))
  }

}
