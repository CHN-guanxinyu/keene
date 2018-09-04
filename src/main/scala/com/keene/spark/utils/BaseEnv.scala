package com.keene.spark.utils
import org.apache.spark.{SparkConf => Sconf}
trait BaseEnv {
  //system env

  def appName: String = this.getClass.getSimpleName.filter(!_.equals('$'))

  def master: String = "yarn"

  def sparkConfOpts: Map[String, String] = Map.empty

  def second: Int = 1


  final implicit def _2str: Any => String = _ toString

  final def sparkConf: Sconf = {

    val cfg = new Sconf().
      setAppName(appName).
      setAll(sparkConfOpts)

    //Win环境下默认local[*]
    if (isWindows) cfg setMaster "local[*]"

    cfg
  }

  final def isWindows: Boolean =
    System.getProperties.
      getProperty("os.name").
      toUpperCase.
      indexOf("WINDOWS") != -1
}
