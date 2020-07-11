package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellConfig

object MaxwellConfigFactory {
  def create(args:Array[String]): MaxwellConfig = {
    new MaxwellConfig(args)
  }
}
