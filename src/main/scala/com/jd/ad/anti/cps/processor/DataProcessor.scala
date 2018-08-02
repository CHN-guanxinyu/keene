package com.jd.ad.anti.cps.processor

import com.jd.ad.anti.cps.ExecutorArgs
import com.jd.ad.anti.cps.filter._
import com.jd.ad.anti.cps.yaml._Filter
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import java.util.ArrayList

class DataProcessor(filters: ArrayList[_Filter]) {
  def process(sc: SparkContext, data: RDD[Map[String, Any]],argv: ExecutorArgs): RDD[Map[String, Any]] = {
    var billData: RDD[Map[String, Any]] = data
    if(filters != null && filters.size() > 0){
      billData = sc.emptyRDD
      for (i <-  0 until filters.size()) {
        val filter = FilterFactory.getFilter(filters.get(i))
        if (filter != null) {
          filter.init(sc, argv)
          val filterResult = filter.doFilter(data)
          //TODO 当击中第一个策略时应该选择跳出
          billData = billData.union(filterResult)
        } else {
          println("could not create instance of " + filters.get(i).filter_type)
        }
      }
    }
    // TODO 没有filter的job不做distinct比较好
    billData.distinct()
  }
}
