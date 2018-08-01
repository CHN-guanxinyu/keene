package com.jd.ads_anti.sync

import com.keene.core.Runner
import com.keene.core.parsers.ArgumentsParser
import com.keene.core.implicits._
class SearchAppPvAssi extends Runner{
  override def run (args: Array[ String ]): Unit = {
    val arg = ArgumentsParser[Args](args)
    val sap = new SearchAppPv
    implicit val date : String = arg.date
    //input
    Map(
      "log_mark"    ->  sap.fetchGdmOnlineLogMark
    ).foreach{ case (table , df) => df.createOrReplaceTempView( table )}

    "select browser_uniq_id , count(1) count from log_mark group by browser_uniq_id order by count desc".go show

  }
}
