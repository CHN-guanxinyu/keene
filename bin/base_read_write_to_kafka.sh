#!/usr/bin/env bash

source bin/run_examples.sh

run \
--klass kafka.BaseReadWriteToKafka \
--brokers localhost:9092 \
--subscribe test \
--topic spark
