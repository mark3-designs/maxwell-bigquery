package com.steckytech.maxwell.kryo

import com.esotericsoftware.kryo.Kryo
import com.zendesk.maxwell.Maxwell
import org.apache.spark.serializer.KryoRegistrator

@Deprecated
object Registrator {

}
@Deprecated
class Registrator extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[Maxwell], new MaxwellSerializer())
  }

}
