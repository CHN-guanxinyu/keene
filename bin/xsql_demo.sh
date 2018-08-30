#!/usr/bin/env bash
LOCAL_JAR=$(ls target/scala*/anti-batch_*.jar)

spark-submit \
--class com.keene.spark.xsql.Main \
--executor-cores 4 \
--executor-memory 4G \
--driver-memory 10G \
--conf spark.sql.shuffle.partitions=2000 \
$LOCAL_JAR \
--ext-class package.to.MyContext \
--config-path conf/xsql_demo/result.xml \
--debug true
