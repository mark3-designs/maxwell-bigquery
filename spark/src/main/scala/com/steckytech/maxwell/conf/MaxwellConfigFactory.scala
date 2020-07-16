package com.steckytech.maxwell.conf

import com.zendesk.maxwell.MaxwellConfig

@Deprecated
class MaxwellConfigFactory extends Serializable {

  val args:List[String] = List[String]()

  def set(configKey:String, value:String): MaxwellConfigFactory = {
    args + ("--"+ configKey +"="+ value)
    this
  }

  def build(args:Array[String]): MaxwellConfig = {
    new MaxwellConfig((this.args ++ List(args)).map(x => x.asInstanceOf[String]).toArray[String])
  }
}

