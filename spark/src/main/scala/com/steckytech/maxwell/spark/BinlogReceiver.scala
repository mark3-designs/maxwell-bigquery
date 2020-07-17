package com.steckytech.maxwell.spark

import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.steckytech.maxwell.mysql.BinlogEvent
import com.zendesk.maxwell.producer.ProducerFactory
import org.apache.spark.storage.StorageLevel


class BinlogReceiver(configFactory: MaxwellConfigFactory, storage:StorageLevel) extends MaxwellReceiver[BinlogEvent](configFactory, storage) with Serializable {

  override def getProducerFactory(): ProducerFactory = {
    new BinlogProducerFactory(this)
  }

}
