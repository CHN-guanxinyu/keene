spark-submit ^
--master "local[*]" ^
--class "com.keene.spark.examples.main.Main" ^
--packages "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1" ^
out/artifacts/keene_jar/keene.jar ^
%*