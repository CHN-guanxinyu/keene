package com.keene.spark.xsql.io.impl

import com.keene.spark.xsql.io.DataManager
import com.keene.spark.xsql.xml.entity.{ResultElement, TableElement}
import org.apache.spark.sql.SparkSession

class HiveDataManager extends DataManager{
  override def load (
    elem: TableElement,
    cache: Boolean
  )(implicit spark: SparkSession): Unit = {
    //不需要做任何事,hive的load直接与CommonTableElement放到一起就可以了
  }

  override def store (elem: ResultElement)(implicit spark: SparkSession): Unit = {
    //TODO
  }
}
