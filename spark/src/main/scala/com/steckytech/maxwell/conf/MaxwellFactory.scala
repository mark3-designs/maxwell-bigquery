package com.steckytech.maxwell.conf

import com.zendesk.maxwell.MaxwellConfig

class MaxwellFactory(args:Array[String]) {

  val config:MaxwellConfig = new MaxwellConfigFactory(args).build

}
