package com.jd.ad.anti.cps.variable

import java.text.SimpleDateFormat
import java.util.Date

import com.jd.ad.anti.cps.util.{FileUtils, JobIni}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ConstantInputDStream

/**
  * Created by haojun on 2017/9/7.
  */
class RealtimeVariables  extends Serializable{

  var varsMap:Map[String, AbstractVariable] = Map()
  var varsPath:Map[String,String] = Map()
  val doneFile = "_SUCCESS"

  def init(ssc:StreamingContext, conf:JobIni) : Unit = {
    val sc = ssc.sparkContext
    val refreshDstream = new  ConstantInputDStream(ssc, sc.parallelize(Seq()))
          .window(Seconds(conf.getCacheUpdateDuration()),Seconds(conf.getCacheUpdateDuration()))

    initClickVariables(conf)

    def getData(sc:SparkContext): Unit = {
      // 获取更新路径

      println("-----------------------------------------")
      println("Start update cache:")
      var time_now:Date = new Date()
      var dateFormat:SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
      var run_day = dateFormat.format( time_now )

      for( (name,path) <- varsPath ){
        if (varsMap.contains(name)) {
          val v = varsMap.get(name)
          val latestPath = getLastPath(path)
          v.get.updateData(sc, latestPath, run_day)

          println("  Update " + name + " with " + latestPath)
        }
      }
      println("End update cache")
      println("-----------------------------------------")

    }

    // Run for first window
    getData(sc)

    refreshDstream.foreachRDD{ rdd =>
      getData(sc)
    }
  }

  def initClickVariables(conf: JobIni): Unit = {

    if (conf.getJobName() == "OrderJoinClickLogFilterJob") {
      val regSite: AbstractVariable = new RegisterSite()
      varsMap += "REGISTER_SITES" -> regSite
      varsPath += "REGISTER_SITES" -> conf.getVarUnionWebPath()
      val socialRegSite: AbstractVariable = new SocialRegisterSite()
      varsMap += "SOCIAL_REGISTER_SITES" -> socialRegSite
      varsPath += "SOCIAL_REGISTER_SITES" -> conf.getVarUnionWebSocialPath()
      val thirdUnion: AbstractVariable = new ThirdpartyUnion()
      varsMap += "THIRDPARTY_LIST" -> thirdUnion
      varsPath += "THIRDPARTY_LIST" -> conf.getVarThirdPartyUnionPath()
      val noRefer: AbstractVariable = new NoReferWhiteList()
      varsMap += "WHITE_LIST" -> noRefer
      varsPath += "WHITE_LIST" -> conf.getVarNoReferWhiteListPath()
      val unionBlackList: UnionBlacklist = new UnionBlacklist()
      varsMap += "UNION_BLACKLIST" -> unionBlackList
      varsPath += "UNION_BLACKLIST" -> conf.getVarUnionBlackListPath()
    }

  }

  def getLastPath(root: String): String = {
    val pat = """.*/20\d+""".r
    FileUtils.getLatestPath(root, (f: String) =>
      FileUtils.exists(f + "/" + doneFile) && pat.findFirstIn(f).isDefined
    )
  }
}
