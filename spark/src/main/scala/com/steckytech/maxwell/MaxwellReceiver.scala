package com.steckytech.maxwell

import java.util.concurrent.BlockingQueue

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.collection.mutable.ArrayBuffer
import scala.reflect.internal.util.Collections

class Metadata() {
  val timestamp:Long = System.currentTimeMillis()
}

class MaxwellReceiver(q:BlockingQueue[String]) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  // val LOG:Logger = SparkRunner.LOGGER
  var thread:Thread = null

  def onStart() {
    println("Startup")

    // Setup stuff (start threads, open sockets, etc.) to start receiving data.
    // Must start new thread to receive data, as onStart() must be non-blocking.

    // Call store(...) in those threads to store received data into Spark's memory.

    // Call stop(...), restart(...) or reportError(...) on any thread based on how
    // different errors need to be handled.

    // See corresponding method documentation for more details


    // Start the thread that receives data over a connection
    this.thread= new Thread("maxwell-receiver") {
      override def run() {
        println("Receiver Thread Initializing")
        while (!isStopped()) {

          val items: ArrayBuffer[String] = new ArrayBuffer[String]()
          var item: String = null
          item = q.poll()
          if (item != null) {
            store(new ArrayBuffer[String]() + item)
            println(items.size + " message stored")
          }
        }
        println("Receiver Thread Stopped")
      }
    }

    this.thread.start()

  }

  def onStop() {
    // Cleanup stuff (stop threads, close sockets, etc.) to stop receiving data.
    if (this.thread != null) {
      this.thread.join()
    }
  }
}
