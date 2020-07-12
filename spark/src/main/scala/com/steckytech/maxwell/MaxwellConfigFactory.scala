package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellConfig

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

/*
object MaxwellConfigFactory {
  def from(args:Array[String]): MaxwellConfig = {
    new MaxwellConfigFactory().build(args)
  }
}
 */
