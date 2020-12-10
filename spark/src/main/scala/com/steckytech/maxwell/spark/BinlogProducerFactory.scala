package com.steckytech.maxwell.spark

import com.steckytech.maxwell.mysql.BinlogEvent
import com.zendesk.maxwell.MaxwellContext
import com.zendesk.maxwell.producer.AbstractProducer
import com.zendesk.maxwell.row.RowMap
import org.codehaus.jackson.map.ObjectMapper

class BinlogProducerFactory(receiver: BinlogReceiver) extends com.zendesk.maxwell.producer.ProducerFactory {

  override def createProducer(maxwell: MaxwellContext): AbstractProducer = {
    new BinlogProducer(receiver, maxwell)
  }

}

class BinlogProducer(receiver: BinlogReceiver, maxwell:MaxwellContext) extends AbstractProducer(maxwell) {
  override def push(r: RowMap): Unit = {
    val item = new ObjectMapper().readValue(r.toJSON(maxwell.getConfig.outputConfig), classOf[Map[String,Object]])
    // TODO: partitioning
    val change = item.get("type").asInstanceOf[String]
    val database = item.get("database").asInstanceOf[String]
    val table = item.get("table").asInstanceOf[String]
    change match {
      case "database-alter" =>
      case "table-drop" =>
      case "table-create" =>
      case "table-alter" =>
      case "insert" =>
    }
    receiver.store(new BinlogEvent(item))
    maxwell.setPosition(r.getNextPosition)
  }

}



