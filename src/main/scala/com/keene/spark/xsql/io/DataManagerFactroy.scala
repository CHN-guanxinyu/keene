package com.keene.spark.xsql.io

import com.keene.spark.xsql.base.AbstractFactory
import com.keene.spark.xsql.xml.entity.Element


object DataManagerFactroy extends AbstractFactory[DataManager]("DataManager"){
  override protected val packageTo = "com.keene.spark.xsql.io.impl"
  def newInstance(elem : Element) =
  //这里是指 AbcdTableElement with SourceSupport 或者 AbcdResultElement 就对应于 AbcdDataManager
    super.newInstance(
      elem.getClass.
        getSimpleName.
        diff("TableResultElement")
    )
}
