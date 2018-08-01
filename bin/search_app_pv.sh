#!/usr/bin/env bash

source bin/ads_anti.sh
JAR=$(ls target/scala*/*.jar)
DATE='2018-07-29'

NUM_EXECUTORS=200
CORES=4
MEM=16G

run \
--queue "root.bdp_jmart_ad.jd_ad_oozie" \
--num-executors ${NUM_EXECUTORS} \
--executor-cores ${CORES} \
--executor-memory ${MEM} \
${JAR} \
--class sync.SearchAppPv \
--date ${DATE} \
--result-table antidb.keen_search_app_pv_log