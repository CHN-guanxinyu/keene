#!/usr/bin/env bash

source bin/ads_anti.sh
JAR=$(ls target/scala*/*.jar)
DATE='2018-07-29'

NUMEXECUTORS=200
CORES=4
MEM=16G

run \
--num-executors ${NUMEXECUTORS} \
--executor-cores ${CORES} \
--executor-memory ${MEM} \
--class sync.SearchAppPv \
--date ${DATE} \
--result-table antidb.keen_search_app_pv_log