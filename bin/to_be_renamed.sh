#!/usr/bin/env bash
DATE=$1
LOCAL_JAR=$(ls target/scala*/*.jar)
spark-submit \
--class com.jd.ad.anti.toberename.ToBeRenamed \
--queue "root.bdp_jmart_ad.jd_ad_oozie" \
--num-executors 100 \
--executor-cores 4 \
--executor-memory 8g \
--conf spark.sql.shuffle.partitions 1200 \
$LOCAL_JAR \
--date $DATE
