#!/usr/bin/env bash

function run(){
    spark-submit \
    --class "com.keene.spark.core.main.Main" \
    $*
}
#--packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1"