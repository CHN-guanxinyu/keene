package com.keene.spark.xsql.io.impl

import com.keene.spark.xsql.io.DataManager
import com.keene.spark.xsql.xml.entity.{ResultElement, TableElement, TextFileResultElement, TextFileTableElement}
import org.apache.spark.sql.SparkSession

class TextFileDataManager extends DataManager{
  override def load (elem: TableElement, cache : Boolean)(implicit spark : SparkSession): Unit = {
    val e = elem.asInstanceOf[TextFileTableElement]
    val (p, sp, sch, n) = (e.path, e.split, e.schema, e.name)

    import spark.implicits._

    planWithCache(
      spark.sparkContext.
        textFile(p).
        mapPartitions(_ map(_ split sp)).
        toDF(sch.split(",") : _*),
      n, cache
    )
  }

  override def store (elem: ResultElement)(implicit spark : SparkSession): Unit = {
    val e = elem.asInstanceOf[TextFileResultElement]
    val (f, p, s, r) = (e.from, e.path, e.split, e.repartitionNum.toInt)
    //TODO
  }
}
