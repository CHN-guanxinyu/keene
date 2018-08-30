package com.keene.spark.xsql.io

import com.keene.spark.xsql.base.ExecutionContext
import com.keene.spark.xsql.xml.entity.{ResultElement, TableElement}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 外部数据操作者
  */
trait DataManager extends ExecutionContext{
  def load(elem : TableElement, cache : Boolean)(implicit spark : SparkSession)
  def store(elem : ResultElement)(implicit spark : SparkSession)


}
