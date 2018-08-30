package com.keene.spark.xsql.base

import com.keene.core.implicits._
import com.keene.spark.xsql.xml.entity.TableElement
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

trait ExecutionContext extends Context{
  protected final implicit lazy val spark = {
    val da = "spark.dynamicAllocation"
    SparkSession.builder.
      config("spark.shuffle.service.enabled",true).
      config(s"$da.enabled",true).
      config(s"$da.minExecutors",10).
      config(s"$da.maxExecutors",400).
      config(s"$da.schedulerBacklogTimeout","1s").
      config(s"$da.sustainedSchedulerBacklogTimeout", "5s").
      enableHiveSupport.
      getOrCreate
  }

  protected def planWithCache(
    sql : String,
    tableElem : TableElement,
    cache : Boolean,
    showIfError : AnalysisException => String
  ) : Unit =
    try{
      val df = sql.go.na.fill(tableElem.nullFill)
      planWithCache(df, tableElem.name, cache)
    } catch {
      case e : AnalysisException =>
        System.err.println(showIfError(e))
        spark.stop
        System exit 1
    }

  protected def planWithCache(
    df : DataFrame,
    name: String,
    cache : Boolean
  ) : Unit = (if(cache) df.cache else df).createOrReplaceTempView(name)

  def extEnv = {}


}
