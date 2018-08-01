package com.keene.spark.examples.hive

import com.keene.core.ExampleRunner
import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._

class BaseReadHiveTest extends SimpleSpark with ExampleRunner{
  override def run (args: Array[ String ]): Unit = {
    val arg = ArgumentsParser[BaseReadHiveArg](args)

//    val df = s"select * from ${arg.tableName} limit 100".go

    val ds = spark.readStream.
      format("socket").
      option("host" , arg.host).
      option("port" , arg.port).
      load

    ds.writeStream.format("console").start
//    ds.join(df , Seq("key_id"))

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