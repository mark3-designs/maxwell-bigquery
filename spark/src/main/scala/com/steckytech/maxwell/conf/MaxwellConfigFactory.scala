package com.steckytech.maxwell.conf

import com.zendesk.maxwell.MaxwellConfig

class MaxwellConfigFactory(cmdArgs:Array[String]) extends Serializable {

  val args:List[String] = cmdArgs.toList

  def set(configKey:String, value:String): MaxwellConfigFactory = {
    args + ("--"+ configKey +"="+ value)
    this
  }

  def build(args:Array[String]): MaxwellConfig = {
    new MaxwellConfig((this.args ++ List(args)).map(x => x.asInstanceOf[String]).toArray[String])
  }

  def build(): MaxwellConfig = {
    new MaxwellConfig(args.toArray[String])
  }
}

