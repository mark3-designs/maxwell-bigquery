# Maxwell-Spark

# Build Dependencies

# Spark-Submit

```
#!/bin/bash

export EXECUTOR_MEM="1024M"

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

SPARK_CLUSTER="local[4]"

spark-submit \
  --num-executors 16 \
  --executor-memory $(EXECUTOR_MEM) \
  --master $SPARK_CLUSTER \
  --class com.steckytech.maxwell.SparkRunner \
  hdfs://hadoop02:9000/user/brad/maxwell-spark-1.0.jar $ARGS
```


# Example

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

