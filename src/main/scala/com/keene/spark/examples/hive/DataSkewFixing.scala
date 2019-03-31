package com.keene.spark.examples.hive

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.core.parsers.{Arguments, ArgumentsParser => Parser}
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.HashSet

class DataSkewFixing extends Runner with SimpleSpark[SparkSession]{

  import spark.implicits._

  override def run (args: Array[ String ]): Unit = {

    implicit val arg = Parser[Args] ( args )

    //加载数据
    //hive表别名,注册临时表
    //table_a, TB级数据, 千万条
    //table_b, GB级数据, 上亿条
    registerInputTable

    val setU = "select distinct join_key_ from table_a".go cache

    chooseBetterBCAndRegisterFunction(setU, "table_b".tab)

    setU.unpersist

    saveResult
  }
  def registerInputTable(implicit arg: Args) =
    List(
      "table_a" -> tableASql.go.cache,
      "table_b" -> tableBSql.go
    ).foreach{ case (alias, df) =>
      df createOrReplaceTempView alias
    }

  def tableASql(implicit arg: Args): String =
    s"""
       |select join_key_,...
       |from db.table_a
       |where dt='${arg.date}'
    """.stripMargin

  def tableBSql(implicit arg: Args): String =
    s"""
       |select join_key_
       |from db.table_b
       |where dt='${arg.date}'
    """.stripMargin

  def broadcastSet(df : DataFrame) =
    df.as[String].collect.to[HashSet] bc

  def chooseBetterBCAndRegisterFunction(setU : DataFrame, setB : DataFrame) ={
    val setI = setU.intersect( setB ).cache
    val setE = setU.except( setB ).cache

    val (setICount, setECount) = (setI.count, setE.count)

    //选择小集合广播
    val useSetI = setICount < setECount
    val betterBc = broadcastSet( if( useSetI ) setI else setE )

    Seq(setI, setE).map(_.unpersist)

    //用setI去做contains，有则能join上，填充1，反之0
    //用setE去做contains，有则说明无法join，则填充0，无则1
    spark.udf register(
      "fillIfJoined", (k : String) =>
      if( useSetI ^ betterBc.value.contains(k) ) 1 else 0
    )
  }

  def saveResult(implicit arg: Args) = {
    saveResultToTempPath
    "uncache table table_b" go
    loadDataIntoHive
  }

  def saveResultToTempPath(implicit arg: Args) =
    """
      |select
      |  table_a.*,
      |  fillIfJoined(join_key_) as fillIfJoined
      |from
      |  table_a
    """.stripMargin.go.
      repartition(arg.numRepartition).
      write.
      mode("overwrite").
      orc(arg.tempPath)

  def loadDataIntoHive(implicit arg: Args) =
    s"""
       |load data inpath '${arg.tempPath}'
       |overwrite into table ${arg.resultTable}
       |partition (dt='${arg.date}')
    """.stripMargin go
}

class Args(
  var numRepartition : Int = 2000,
  var date : String = "",
  var resultTable : String = "",
  var tempPath : String = ""
) extends Arguments {
  override def usage =
    """
      |Options:
      |
      |--date
      |--num-repartition
      |--temp-path        写结果表前首先存储到hdfs的路径
      |--result-table     结果表
    """.stripMargin
}