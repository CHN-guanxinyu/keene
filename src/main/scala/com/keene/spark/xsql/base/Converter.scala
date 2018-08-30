package com.keene.spark.xsql.base

import org.apache.spark.sql.DataFrame

trait Converter extends ExecutionContext{
  def convert(table : String) : DataFrame
}
