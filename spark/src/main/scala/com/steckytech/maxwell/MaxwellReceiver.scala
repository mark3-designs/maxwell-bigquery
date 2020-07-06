package com.steckytech.maxwell

import com.zendesk.maxwell.MaxwellContext
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer

class Metadata() {
  val timestamp:Long = System.currentTimeMillis()
}

object MaxwellReceiver {

}

class MaxwellReceiver(maxwell:MaxwellContext) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  var thread:Thread= null
  var running:Boolean= false
  var q:Seq[String]= null

  def init(sc: SparkContext, stream: StreamingContext, queue: Seq[String]):Unit = {
    this.q = queue
  }

  def onStart() {
    // Setup stuff (start threads, open sockets, etc.) to start receiving data.
    // Must start new thread to receive data, as onStart() must be non-blocking.

    // Call store(...) in those threads to store received data into Spark's memory.

    // Call stop(...), restart(...) or reportError(...) on any thread based on how
    // different errors need to be handled.

    // See corresponding method documentation for more details


    // Start the thread that receives data over a connection
    running= true
    thread= new Thread("maxwell-receiver") {
      override def run() {
        receive()
      }
    }

    thread.start()


  }

  private def receive(): Unit = {
    while (running) {
      q match {
        case q if (q != null) =>
          val items: Seq[String] = q.take(q.size)
          store(ArrayBuffer[String]() ++ items.toArray)
      }
    }
  }

  def onStop() {
    // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
    maxwell.terminate()
    running= false
    thread.join()
  }
}
