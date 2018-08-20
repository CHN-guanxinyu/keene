#!/usr/bin/env bash
DATE=$1
JAR=$(ls target/scala*/*.jar)
source bin/run_examples.sh

NUM_EXECUTORS=400
CORES=2
MEM=10G
REDUCERS=$((NUM_EXECUTORS * CORES * 3))

run \
--queue "root.bdp_jmart_ad.jd_ad_oozie" \
--num-executors ${NUM_EXECUTORS} \
--executor-cores ${CORES} \
--executor-memory ${MEM} \
--conf spark.yarn.executor.memoryOverhead=10240 \
--conf spark.sql.codegen=true \
--conf spark.sql.shuffle.partitions=$REDUCERS \
${JAR} \
--class sync.SearchAppPvAssi \
--date ${DATE} \
