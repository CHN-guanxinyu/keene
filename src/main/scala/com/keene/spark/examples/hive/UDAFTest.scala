package com.keene.spark.examples.hive
import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class UDAFTest extends Runner with SimpleSpark[SparkSession] {
  override def run (implicit args: Array[ String ]): Unit = {
    "select * from (select 1 id, 1 key,3 value) union ( select 1 id, 2 key, 4 value) ".go createOrReplaceTempView "t"
    spark.udf.register("foo", MyUdaf)
    "select foo(key, value) from t group by id".go show false
  }
}
case object MyUdaf extends UserDefinedAggregateFunction{
  override def inputSchema = StructType(Seq(
    StructField("key", IntegerType),
    StructField("value", IntegerType)
  ))

  override def bufferSchema = StructType(StructField("result", IntegerType) :: Nil)

  override def dataType = IntegerType

  override def deterministic = true

  override def initialize (buffer: MutableAggregationBuffer) =  buffer.update(0, 0)


  override def update (
    buffer: MutableAggregationBuffer,
    input: Row
  ){
    val old = buffer getInt 0
    val addi = (input getInt 0) * (input getInt 1)
    buffer.update(0, old + addi)
  }

  override def merge (
    buffer1: MutableAggregationBuffer,
    buffer2: Row
  )= buffer1.update(0, (buffer1 getInt 0) + (buffer2 getInt 0) )

  override def evaluate (buffer: Row) = buffer getInt 0
}
