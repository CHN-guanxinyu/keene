package com.jd.ad.anti.cps.extractor

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.jd.ad.anti.cps.realtime.{CheckPointHolder, Statistic}
import com.jd.ad.anti.cps.util.JobIni
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{ConstantInputDStream, DStream}

/**
  * Created by haojun on 2017/9/25.
  */
class RealtimeFraudClickLog  extends RealtimeDataExtractor {

  def getDStream(ssc: StreamingContext, conf: JobIni, statistic: Statistic): DStream[Map[String, Any]] = {
    val inputFileStream = ssc.fileStream[LongWritable,Text,TextInputFormat](
                            conf.getUnionFraudClickOutputPath(),
                            filterPath(_),
                            false
                          ).map(_._2.toString)

    val fraudClkStream = inputFileStream.window(Duration(conf.getCacheFraudClickWindowDuration()*1000))
                                                .map{row =>
      val a = row.split(",")
      if (a.length == 3) {
        Map(
          "click_id" -> a(0),
          "policy_id" -> a(1).toInt,
          "click_seconds" -> a(2).toLong
        )
      }else{
        null
      }
    }

    val fraudClkStreamTF = fraudClkStream.transform{ rdd =>
      val runningFlag = new Path(conf.getHdfsRootPath() + "/_PROCESS")
      val fs = runningFlag.getFileSystem(new Configuration())
      if (!fs.exists(runningFlag)) fs.createNewFile(runningFlag)
      rdd
    }

    fraudClkStreamTF

//    getTestFraudClickRDD(ssc, conf)
  }

  /*
  程序启动前需手动清除根目录_PROCESS 文件，当程序启动后会判断根目录_PROCESS 文件是否存在，
  如果不存在则读取15天分小时目录数据，并在根目录中添加_PROCESS 文件
  如果存在则使用实时文件目录，读取新加入的数据
  目录结构如下：
  /_PROCESS
  /cps_fraud_union_click/hourly_yyyyMMdd
  /cps_fraud_union_click/yyyyMMddHHmmSS
   */
  def filterPath(path: Path): Boolean = {
    val fs = path.getFileSystem(new Configuration())
    val init_flag = new Path(path.getParent.getParent.getParent.toString + "/_PROCESS")
    var filter_flag = true

    val partPat = """.*/part-\d+""".r
    if (partPat.findFirstIn(path.toString).isEmpty) filter_flag = false

    if (fs.exists(init_flag)) {
      val pat = """.*/hourly_20\d+/.*""".r
      if (pat.findFirstIn(path.toString).isDefined) filter_flag = false
    } else {
      println("initial adding hourly dir: " + path)
    }
    filter_flag
  }

  def getTestFraudClickRDD(ssc:StreamingContext, conf:JobIni) : DStream[Map[String,Any]] = {
    val ts = new Timestamp(System.currentTimeMillis())
    var rowData: Array[Map[String, Any]] = Array()
    //    # 过滤京乐享和京享街(17, 44),target_url不匹配判作弊
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d6",
      "policy_id" -> 2001,
      "click_seconds" -> ts.getTime/1000
    )
    rowData :+= Map(
      "click_id" -> "9ef3a4c4bf5541f5afeedb83e0c165d5",
      "policy_id" -> 201,
      "click_seconds" -> ts.getTime/1000
    )
    val testDStream = new  ConstantInputDStream(ssc, ssc.sparkContext.parallelize(rowData))
      .window(Seconds(conf.getJobFrequency()),Seconds(conf.getJobFrequency()))

    testDStream
  }

}
