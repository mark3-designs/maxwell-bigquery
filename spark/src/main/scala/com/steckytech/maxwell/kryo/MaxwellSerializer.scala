package com.steckytech.maxwell.kryo

import com.esotericsoftware.kryo.io.{Input, Output}
import com.zendesk.maxwell.{Maxwell, MaxwellConfig}
import com.esotericsoftware.kryo.{Kryo, Serializer}

class MaxwellSerializer extends Serializer[MaxwellConfig] {

  override def write(kryo: Kryo, output: Output, obj: MaxwellConfig): Unit = {
    kryo.writeObject(output, obj)
  }

  override def read(kryo: Kryo, input: Input, aClass: Class[MaxwellConfig]): MaxwellConfig = {
    kryo.readObject(input, aClass)
  }
}

