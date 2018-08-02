package com.jd.ad.anti.cps.dumper

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import com.jd.ad.anti.cps.ExecutorArgs
abstract class DataDumper {
  def saveData(sc: SparkContext, data: RDD[Map[String, Any]], argv: ExecutorArgs)
  
  def dfSchema(columnNames: List[String]): StructType = {
    val fields = columnNames.map { x =>
      StructField(name = x, dataType = StringType, nullable = false)
    }
    StructType(fields)
  }
  
  def saveAsTable(sc: SparkContext,
    data: RDD[Map[String, Any]],
    partitionPath: String,
    tableName: String,
    partitions: String,
    repartitionNum: Int,
    columnNames: List[String]) = {
    val bcColumnsNames = sc.broadcast(columnNames)
    val dataset = data.map { row =>
      val columnNames = bcColumnsNames.value
      Row.fromSeq( columnNames.map { name => 
        val v = row.getOrElse(name, "")
        if (v == null) "" else v.toString
      })
    }

    println("save count " + dataset.count)
    val sparkSession = SparkSession.builder.getOrCreate()
    sparkSession.createDataFrame(dataset, dfSchema(columnNames))
      .repartition(repartitionNum)
      .write.mode("overwrite")
      .option("sep", "\t")
      .csv(partitionPath)

    val addPartitionSqlString = s"""
    alter table ${tableName} add if not exists
    partition(${partitions})
    """

    val sqlContext = new HiveContext(sc)
    sqlContext.sql(addPartitionSqlString)
  }
}
