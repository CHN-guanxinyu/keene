#!/usr/bin/env bash

function run(){
    spark-submit \
    --class "com.jd.ads_anti.Main" \
    $*
}