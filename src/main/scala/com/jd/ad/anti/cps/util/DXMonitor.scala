package com.jd.ad.anti.cps.util

import java.sql.{PreparedStatement, DriverManager, Connection, Timestamp}
import org.slf4j.{LoggerFactory, Logger}
    

class DXMonitor(host: String, db: String, table: String, fields: String, user: String, passwd: String) {
  val log: Logger = LoggerFactory.getLogger(this.getClass)
  val sql: String = sqlGenerate(table, fields)
  var conn: Connection = null
  val pst: PreparedStatement = newPrepareStatement()

  def newPrepareStatement(): PreparedStatement ={
      try{
        Class.forName("com.mysql.jdbc.Driver")
        conn = DriverManager.getConnection(getJdbcUrl(host, db), user, passwd)
      } catch {
        case e: Exception => {
          log.warn("Failed to get connection, caused by:" + e.getCause)
        }
      }
      conn.prepareStatement(sql)
  }

  def getJdbcUrl(host: String, db: String):String = {
    "jdbc:mysql://%s/%s".format(host, db)
  }

  def report(timestamp:Timestamp, values: Array[String]):Unit = {
    pst.setTimestamp(1,timestamp)
    val idx: Seq[Int] = Range(0, values.length)
    idx.map(i => pst.setString(i+2, values(i)))
    //log.warn("values: "+ values.mkString(" ,"))
    
    pst.addBatch
    pst.executeBatch
    log.warn("current write: " + values.mkString(" ,"))
  }
  
  def sqlGenerate(tableName: String, fields: String): String = {
    val insertValues = fields.split(",").map(f => "?").mkString(" ,")
    "replace into %s (%s) values (%s)".format(tableName, fields, insertValues)
  }
}
