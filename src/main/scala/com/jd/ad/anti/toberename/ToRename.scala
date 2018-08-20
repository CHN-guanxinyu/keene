package com.jd.ad.anti.toberename
import com.jd.ad.anti.utils.implicits._
import com.jd.ad.anti.utils.parsers.Arguments
import org.apache.spark.sql.SparkSession
object ToBeRenamed {
  val spark = SparkSession.builder.enableHiveSupport.getOrCreate
  import spark.implicits._
  def main (args: Array[ String ]): Unit = {
    val arg = args.as[ToBeRenamedArg]
    val click = "ad.ad_base_click".fetchAll.where($"dt" === arg.date)
//    val order = "ad.ad_ads_swc_order_detail_snapshot_log".fetchAll

    click.select("utm_term").show
  }
}

class ToBeRenamedArg(var date : String = "") extends Arguments {
  override def usage =
    """Options:
      |
      |--date
    """.stripMargin
}
