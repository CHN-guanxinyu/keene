package com.jd.ad.anti.cps

import java.util.Date
import java.util.Calendar
import java.text.SimpleDateFormat

case class InvalidArgException(smth: String) extends Exception

class ExecutorArgs(argv: Array[String]) extends Serializable {
  
  var day: Date = {
    var cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    cal.getTime
  }
  var beginDay = day
  var nextDay = day
  var jobName: String = ""
  var yamlFile: String = ""
  var outputDB: String = ""
  var outputTable: String = ""
  var outputPath: String = ""
  var inputUnionWeb: String = ""
  var inputUnionWebSocial: String = ""
  var inputUnionThirdParty: String = ""
  var inputUnionBlacklist: String = ""
  var inputNoreferWhitelist: String = ""
  var inputWeiboWhitelist: String = ""
  var inputAdtWhitelist: String = ""
  var inputInnerWhitelist: String = ""
  var inputReturnKeyWords: String = ""
  var inputAddressSite: String = ""
  var exportPolicyIds: String = ""
  parse(argv.toList)
  
  private def parse(args: List[String]): Unit = args match {
    case ("--day") :: value :: tail =>
      day = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      beginDay = day
      parse(tail)
      
    case ("--begin-day") :: value :: tail =>
      beginDay = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)

    case ("--next-day") :: value :: tail =>
      nextDay = new SimpleDateFormat("yyyy-MM-dd").parse(value)
      parse(tail)
    
    case ("--job-name") :: value :: tail =>
      jobName = value.toString
      parse(tail)

    case ("--yaml-file") :: value :: tail =>
      yamlFile = value.toString
      parse(tail)

    case ("--output-path") :: value :: tail =>
      outputPath = value.toString
      parse(tail)

    case ("--database") :: value :: tail =>
      outputDB = value.toString
      parse(tail)

    case ("--output-table") :: value :: tail =>
      outputTable = value.toString
      parse(tail)

    case ("--input-union-web") :: value :: tail =>
      inputUnionWeb = value.toString
      parse(tail)

    case ("--input-union-web-social") :: value :: tail =>
      inputUnionWebSocial = value.toString
      parse(tail)

    case ("--input-union-third-party") :: value :: tail =>
      inputUnionThirdParty = value.toString
      parse(tail)

    case ("--input-union-blacklist") :: value :: tail =>
      inputUnionBlacklist = value.toString
      parse(tail)

    case ("--input-norefer-whitelist") :: value :: tail =>
      inputNoreferWhitelist = value.toString
      parse(tail)

    case ("--input-weibo-whitelist") :: value :: tail =>
      inputWeiboWhitelist = value.toString
      parse(tail)

    case ("--input-adTrafficType-whitelist") :: value :: tail =>
      inputAdtWhitelist = value.toString
      parse(tail)

    case ("--input-inner-whitelist") :: value :: tail =>
      inputInnerWhitelist = value.toString
      parse(tail)

    case ("--input-return-keywords") :: value :: tail =>
      inputReturnKeyWords = value.toString
      parse(tail)

    case ("--input-address-site") :: value :: tail =>
      inputAddressSite = value.toString
      parse(tail)
      
    case ("--Export-policy-ids") :: value :: tail =>
      exportPolicyIds = value.toString
      parse(tail)

    case Nil =>
      
    case x =>
      System.err.println(s"Unknown argument: $x")
      throw new InvalidArgException(s"Unknown argument: $x")
  }
}
