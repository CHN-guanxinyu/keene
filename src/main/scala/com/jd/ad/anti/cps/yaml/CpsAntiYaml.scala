package com.jd.ad.anti.cps.yaml

import java.util.ArrayList
import java.util.HashMap

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty
/**
  * Created by Jun on 2017/7/15.
  */
object CpsAntiYaml {
  private var config: _JobGroup = null

  def initialize(path: String) {
    val stream = getClass.getResourceAsStream(path)
    val yaml = new Yaml(new Constructor(classOf[_JobGroup]))
    config = yaml.load(stream).asInstanceOf[_JobGroup]
  }
  def getJobByName(name: String): _Job = {
    for (i <- 0 until config.jobs.size) {
      if (config.jobs.get(i).name.equals(name)) return config.jobs.get(i)
    }
    null
  }
}
class _JobGroup {
  @BeanProperty var jobs: ArrayList[_Job] = null
}
class _Job {
  @BeanProperty var name: String = ""
  @BeanProperty var input: String = ""
  @BeanProperty var output: String = ""
  @BeanProperty var filters: ArrayList[_Filter] = null
}

class _Filter {
  @BeanProperty var filter_type: String = ""
  @BeanProperty var policy_id: Int = -1
  @BeanProperty var variables: HashMap[String, String] = new HashMap[String, String]()
  @BeanProperty var rules: ArrayList[_Rule] = new ArrayList[_Rule]()
}

class _Rule {
  @BeanProperty var space: String = ""
  @BeanProperty var group: _Group = new _Group()
  @BeanProperty var click_time_diff_range: _ClickTimeDiffRange = new _ClickTimeDiffRange()
  @BeanProperty var filter_space: String = ""
}

class _Group {
  @BeanProperty var group_key: String = ""
  @BeanProperty var sort_key: String = ""
}

class _ClickTimeDiffRange {
  @BeanProperty var lower_seconds: Int = 0
  @BeanProperty var upper_seconds: Int = 0
}
