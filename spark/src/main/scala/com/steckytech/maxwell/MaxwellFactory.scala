package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellConfig

class MaxwellFactory(args:Array[String]) {

  val config:MaxwellConfig = new MaxwellConfigFactory().build(args)

}
