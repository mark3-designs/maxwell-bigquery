package com.steckytech.maxwell.spark

import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.zendesk.maxwell.producer.ProducerFactory
import org.apache.spark.storage.StorageLevel


class JSONReceiver(configFactory: MaxwellConfigFactory, storage:StorageLevel = StorageLevel.MEMORY_ONLY_2) extends MaxwellReceiver[String](configFactory, storage) with Serializable {

  override def getProducerFactory(): ProducerFactory = {
    new JSONProducerFactory(this)
  }


}
