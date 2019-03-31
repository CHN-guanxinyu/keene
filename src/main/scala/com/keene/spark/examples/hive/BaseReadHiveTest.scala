package com.keene.spark.examples.hive

import com.keene.core.Runner
import com.keene.core.implicits._
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import org.apache.spark.sql.SparkSession

class BaseReadHiveTest extends SimpleSpark[SparkSession] with Runner{
  override def run (implicit args: Array[ String ]): Unit = {
    val arg = args.as[BaseReadHiveArg]._1

    val df = s"select * from ${arg.tableName} limit 100".go

    val ds = spark.readStream.
      format("socket").
      option("host" , arg.host).
      option("port" , arg.port).
      load

    ds.join(df , Seq("key_id")).writeStream.format("console").start

    spark.streams.awaitAnyTermination
  }
}
class BaseReadHiveArg(
  var host : String = "",
  var port : Int = 0 ,
  val tableName : String = ""
) extends Arguments {
  override def usage =
    """
      |Options:
      |
      |--host       *
      |--port       *
      |--table-name *
    """.stripMargin
}