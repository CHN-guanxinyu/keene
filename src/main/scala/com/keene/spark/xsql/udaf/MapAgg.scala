package com.keene.spark.xsql.udaf

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MapAgg extends UserDefinedAggregateFunction {
  override def inputSchema = StructType(StructField("result", MapType(StringType, DoubleType) ) :: Nil)

  override def bufferSchema = StructType(StructField("temp_result", MapType(StringType, DoubleType) )::Nil)

  override def dataType = MapType(StringType, DoubleType)

  override def deterministic = true

  override def initialize (buffer: MutableAggregationBuffer): Unit = buffer(0) = Map[String,Double]()

  override def update (
    buffer: MutableAggregationBuffer,
    input: Row
  ) = buffer(0) = getMap(buffer) ++ getMap(input)


  override def merge (
    buffer1: MutableAggregationBuffer,
    buffer2: Row
  ) = buffer1(0) = getMap(buffer1) ++ getMap(buffer2)

  override def evaluate (buffer: Row) = getMap(buffer)

  private def getMap(buffer : MutableAggregationBuffer) =
    buffer.getAs[Map[String,Double]](0)

  private def getMap(row : Row) ={
    val res = row.getAs[Map[String,Double]](0)
    if(res == null) Map[String,Double]()
    else res
  }


}