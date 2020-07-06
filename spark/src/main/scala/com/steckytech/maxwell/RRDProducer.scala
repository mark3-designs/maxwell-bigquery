package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap

object RRDProducer {

}

class RRDProducer(maxwell:MaxwellContext) extends AbstractProducer(maxwell)  {

  val queue:Seq[String] = Seq()

  override def push(r: RowMap): Unit = {
    queue ++ Seq(r.toJSON(maxwell.getConfig.outputConfig))
  }

}
