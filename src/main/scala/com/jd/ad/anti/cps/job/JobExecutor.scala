package com.jd.ad.anti.cps.job

import com.jd.ad.anti.cps.ExecutorArgs
import com.jd.ad.anti.cps.extractor._
import com.jd.ad.anti.cps.processor._
import com.jd.ad.anti.cps.dumper._
import com.jd.ad.anti.cps.yaml.{CpsAntiYaml, _Job}
import org.apache.spark.SparkContext

class JobExecutor(argv: ExecutorArgs) {
  var cache: Map[String, Any] = _

  CpsAntiYaml.initialize(argv.yamlFile)

  def run(sc: SparkContext, jobName: String) {

    val jobConf: _Job = CpsAntiYaml.getJobByName(jobName)


    val dataExtractor = Class.forName(jobConf.input)
      .getConstructor()
      .newInstance()
      .asInstanceOf[DataExtractor]

    val dataDumper = Class.forName(jobConf.output)
      .getConstructor()
      .newInstance()
      .asInstanceOf[DataDumper]

    val dataProcessor = new DataProcessor(jobConf.filters)

    val dataIn = dataExtractor.getRDD(sc, argv)
    val dataOut = dataProcessor.process(sc, dataIn,argv)
//    println("input data count: " + dataIn.count)
    dataDumper.saveData(sc, dataOut,argv)

  }
}
