@echo off

call bin/run_examples ^
--klass kafka.BaseReadWriteToKafka ^
--brokers localhost:9092 ^
--subscribe test ^
--topic spark
