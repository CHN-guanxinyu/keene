#!/usr/bin/env bash

source bin/run_examples.sh
JAR=$(ls target/scala*/*.jar)
run \
$JAR \
--klass hive.BaseReadHiveTest \
--host localhost \
--port 9999
