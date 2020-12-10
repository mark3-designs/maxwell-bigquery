export EXECUTOR_MEM="800M"
export ARGS=--schema_user=brad --schema_password=this4now --schema_host=mysql.cyberdyne --replication_user=brad --replication_password=this4now --replication_host=mysql.cyberdyne --replication_port=3306 --gtid_mode=false --log_level=trace --replication_jdbc_options=serverTimezone=US/Mountain --schema_jdbc_options=serverTimezone=US/Mountain --jdbc_options=serverTimezone=US/Mountain --producer=stdout --producer_partition_by=database --output_ddl=true --host=hadoop04 --port=4406 --user=root --password=maxwell


test-maxwell:
	cd maxwell && mvn clean test

clean:
	cd maxwell && mvn clean -DskipTests
install:
	cd maxwell && mvn install -DskipTests
build:
	gradle build

submit:
	hdfs dfs -rm maxwell-spark-1.0.jar; hdfs dfs -cp file://$(PWD)/spark/build/libs/*.jar maxwell-spark-1.0.jar
	spark-submit --num-executors 6 --executor-memory $(EXECUTOR_MEM) --master yarn --class com.steckytech.maxwell.SparkRunner  hdfs://hadoop02:9000/user/brad/maxwell-spark-1.0.jar $(ARGS)
	#spark-submit --executor-memory $(EXECUTOR_MEM) --master yarn --class com.steckytech.maxwell.SparkRunner  hdfs://hadoop02:9000/user/brad/maxwell-spark-1.0.jar $(ARGS)





.PHONY:	build install test-maxwell
