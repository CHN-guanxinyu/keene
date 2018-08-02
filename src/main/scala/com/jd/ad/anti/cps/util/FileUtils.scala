package com.jd.ad.anti.cps.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import scala.io.Source
import scala.util.matching.Regex

/**
  * Created by zhujian on 2016/11/25.
  */
object FileUtils {
  /*
  * This FileUtils is used on driver node. So the default FileSystem is used
  * */
  //todo
  def getDefaultFS(path: String): FileSystem = {

    new Path(path).getFileSystem(new Configuration)
  }

  def exists(path: String): Boolean = {
    val fs = getDefaultFS(path)
    fs.exists(new Path(path))
  }

  def readTextFile(path: String): String = {
    val fs = getDefaultFS(path)
    var is: FSDataInputStream = null
    try {
      is = fs.open(new Path(path))
      Source.fromInputStream(is).getLines().mkString("")
    } finally {
      if (is != null) {
        is.close()
      }
    }
  }

  def writeTextFile(path: String, content: String): Unit = {
    val fs = getDefaultFS(path)
    var os: FSDataOutputStream = null
    try {
      os = fs.create(new Path(path))
      os.writeBytes(content)
    } finally {
      if (os != null) {
        os.close()
      }
    }
  }

  def touchDoneFile(path: String): Unit = {
    val fs = getDefaultFS(path)
    fs.createNewFile(new Path(path))
  }


  def getLatestPath(root: String, f: (String) => Boolean): String = {
    val fs = getDefaultFS(root)
    val sortedFiles = fs.listStatus(new Path(root)).map(_.getPath.toString).sortWith(_.compareTo(_)>0)

    for (file <- sortedFiles) {
      if (f(file)) { return file }
    }
    null
  }

  def delete(path: String): Unit = {
    val fs = getDefaultFS(path)
    fs.delete(new Path(path), true)
  }
}
