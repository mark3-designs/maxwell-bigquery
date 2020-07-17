package com.steckytech.maxwell.mysql

/**
 * Represents a binlog event record from Maxwell that has been serialized to a Map[String,Object] representation from JSON
 *
 * This is the object used to carry the binlog information from the MaxwellReceiver to each task executor during stream processing
 * @param data
 */
class BinlogEvent(data:Map[String,Object]) extends Serializable {

  override def toString() = {
    data.toString()
  }

}
