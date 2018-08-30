package com.keene.spark.xsql.io.impl

import com.keene.spark.xsql.io.DataManager
import com.keene.spark.xsql.xml.entity.{HDFSTableElement, HdfsResultElement, ResultElement, TableElement}
import org.apache.spark.sql.SparkSession
import com.keene.core.implicits._
class HdfsDataManager extends DataManager{
  override def load (elem: TableElement, cache : Boolean)(implicit spark : SparkSession): Unit = {
    val e = elem.asInstanceOf[HDFSTableElement]
    val (p, f, n) = (e.path, e.format, e.name)

    planWithCache(
      spark.read.format(f).load(p),
      n, cache
    )
  }

  override def store (elem: ResultElement)(implicit spark : SparkSession): Unit = {
    val e = elem.asInstanceOf[HdfsResultElement]
    val (fr, p, fmt, r) = (e.from, e.path, e.format, e.repartitionNum.toInt)

    fr.tab.repartition(r).
      write.
      mode("overwrite").
      format(fmt).
      save(p)
  }
}
