package com.jd.ad.anti.cps

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import job._


object SparkExecutor {
  def main(args: Array[String]){
    val argv = new ExecutorArgs(args)
    
    val conf = new SparkConf().setAppName(argv.jobName + "_" + argv.day)
    
    val sc = new SparkContext(conf)

    val executor = new JobExecutor(argv)
    executor.run(sc, argv.jobName)
  }

}
