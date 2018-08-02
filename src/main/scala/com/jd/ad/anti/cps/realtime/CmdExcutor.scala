package com.jd.ad.anti.cps.realtime

import com.jd.ad.anti.cps.util.{FileUtils, JobIni}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by hanlu on 2017/5/15.
  */
class CmdExecutor(config:JobIni, ssc:StreamingContext) {
  val log: Logger = LoggerFactory.getLogger(this.getClass)

  /*
    * the stop cmd is used to kill the app automatically
    * when the app finished the current batch ,it will check the stop cmd file
    * if the stop cmd file is exists,the app will exit(), not going on
     * else continue the next batch
    * */

  val STOP_CMD = "stop_cmd"

  private def stopCmdFile():String = {
    config.getJobConfPath + "/" + STOP_CMD
  }

  def checkStopCmd() = {
    if(FileUtils.exists(stopCmdFile())){
      log.warn("Found the StopCmd. the app will exit !")
      FileUtils.delete(stopCmdFile())
      Quitter.quit(30, ssc, "Stop Spark Job")
    }else{
      log.warn("No Stop Cmd. Just normally start the next batch")
    }
  }
}
