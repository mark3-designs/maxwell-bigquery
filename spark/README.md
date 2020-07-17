# Maxwell-Spark


# Build Dependencies

This spark project for Maxwell depends on a forked build of [Maxwell](https://github.com/mark3-designs/maxwell/tree/mark3-enhancements)

## Build local Maxwell artifact

```
cd ../maxwell && mvn clean install -DskipTests
```

# Spark-Submit

```
#!/bin/bash

EXECUTOR_MEM="1024M"
SPARK_CLUSTER="local[4]"
EXECUTOR_COUNT=4

ARGS="
--schema_user=maxwell
--schema_password=pass
--schema_host=mysql
--schema_port=3306
--schema_jdbc_options=serverTimezone=US/Mountain

--replication_user=maxwell
--replication_password=pass
--replication_host=mysql02
--replication_port=3307
--replication_jdbc_options=serverTimezone=US/Mountain

--gtid_mode=false
--output_ddl=true

--host=maxwelldb
--port=4406
--user=maxwell
--password=maxwell
--jdbc_options=serverTimezone=US/Mountain
"


spark-submit \
  --num-executors $EXECUTOR_COUNT \
  --executor-memory $EXECUTOR_MEM \
  --master $SPARK_CLUSTER \
  --class com.steckytech.maxwell.SparkRunner \
  /path-to/maxwell-spark-1.0.jar $ARGS
```


# Simple Example

```
import com.steckytech.maxwell.conf.MaxwellConfigFactory
import com.steckytech.maxwell.spark.JSONReceiver
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


object SparkRunner {

  val LOGGER = LoggerFactory.getLogger(SparkRunner.getClass)

  def main(args:Array[String]):Unit = {
    LOGGER.info("Starting Maxwell Stream...");

    val session:SparkSession= SparkSession.builder()
      .appName("maxwell")
      .config("spark.driver.memory", "512g")
      .config("spark.executor.memory", "512m")
      .master("local[4]")
      .getOrCreate()

    val destination = "binlog-stream"

    val batchDuration = Seconds(30)

    val streamingContext = new StreamingContext(session.sparkContext, batchDuration)

    // create maxwell receiver
    val receiver = new JSONReceiver(new MaxwellConfigFactory(args))

    // create stream
    val stream = streamingContext.receiverStream(receiver)

    // write to hdfs/file output
    stream.saveAsTextFiles(destination, "json")

    streamingContext.start()

    streamingContext.awaitTermination()

  }

}
```

# Notes

## Checkout git-submodules
This project contains git-submodules, be sure to initialize them prior to building.

```
git submodule update --init
```
