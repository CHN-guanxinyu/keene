package com.keene.core.implicits

import java.nio.charset.{Charset, StandardCharsets}

import com.keene.core.parsers.{Arguments, ArgumentsParser}
import com.keene.kafka.KafkaParam
import com.keene.spark.utils.SimpleSpark
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.math.{min, random}
import scala.reflect.ClassTag
import scala.util.Try
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  * @param t
  * @tparam T
  */
case class AnyImplicitor[T](@transient t : T)(implicit tag : ClassTag[T]){
  def bc(implicit sc : SparkContext) = sc broadcast t
}

case class DataSetImplicitor[T](@transient ds : Dataset[T]) {

  def view(name : Symbol) : Unit = view(name.name)
  def view(name : String) : Unit = ds createOrReplaceTempView name

  def toConsole : StreamingQuery = toConsole("append", Trigger.ProcessingTime(1000))
  def toConsole(mode : String, trigger : Trigger) : StreamingQuery  =
    ds.writeStream.
      format("console").
      trigger(trigger).
      outputMode(mode).
      start

  def toKafka(brokers : String, topic : String, extOpts : Map[String, String] = Map.empty[String,String]) : StreamingQuery=
    toKafka(KafkaParam(brokers, topic, "writer", extOpts))

  def toKafka(implicit kafkaParam: KafkaParam) : StreamingQuery =
    ds.writeStream.
      options( kafkaParam.get ).
      format("kafka").
      start
}

case class SparkSessionImplicitor(@transient spark : SparkSession){

  def fromKafka(brokers : String, subscribe : String, extOpts : Map[String, String] = Map.empty[String,String]) : DataFrame =
    fromKafka(KafkaParam(brokers, subscribe, "reader", extOpts))

  def fromKafka(implicit kafkaParam: KafkaParam ): DataFrame =
    spark.
      readStream.
      format("kafka").
      options( kafkaParam.get ).
      load

  /**
    * load gzip files in given path of default fs
    *
    * Usage:
    * spark gzipDF "path/to/file"
    * @param path
    * @param partitions
    * @return
    */

  def gzipDF(path : String , partitions : Int = 10): DataFrame ={
    import spark.implicits._
    spark.sparkContext.
      binaryFiles( path , partitions ).
      map(_._2).
      flatMap( extractFiles(_).toOption ).
      flatMap( _.map{ case(file , content) => ( file , decode(content) ) } ).
      toDF("file_name_" , "content_")
  }

  private def extractFiles(ps: PortableDataStream, n: Int = 1024) = Try {
    val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
    Stream.continually(Option(tar.getNextTarEntry))
      // Read until next exntry is null
      .takeWhile(_.isDefined).flatten
      // Drop directories
      .filter(!_.isDirectory)
      .map(e => {
        (e.getName,
          Stream.continually {
            // Read n bytes
            val buffer = Array.fill[Byte](n)(-1)
            val i = tar.read(buffer, 0, n)
            (i, buffer take i)
          }
            // Take as long as we've read something
            .takeWhile(_._1 > 0).flatMap(_._2)
            .toArray)})
      .toArray
  }

  private def decode( bytes: Array[Byte] , charset: Charset = StandardCharsets.UTF_8) =
    new String(bytes, StandardCharsets.UTF_8)
}

case class StringImplicitor(@transient str : String) extends SimpleSpark{

  /**
    * Usage: "package.to.classA".as[classA].foo.bar
    *
    * @tparam T
    * @return
    */
  def as[T] = Class.forName( str ).getConstructor().newInstance().asInstanceOf[T]

  /**
    * Usage: "select something".go where "cond1" show false
    * @return
    */

  def go: DataFrame = spark sql str

  def runSql = go

  /**
    * easy way to fetch data of a table or a view
    * @return
    */

  def tab: DataFrame = s"select * from $str" go

  def removeBlanks = replaceBlanks("")
  def replaceBlanks(): String = replaceBlanks(" ")
  def replaceBlanks(replacement : String): String = {
    str.replaceAll("\t|\r|\n", replacement).replaceAll(" {2,}", replacement).trim
  }

  def info = logger info str
  def debug = logger debug str
  def warn = logger warn str
  def error = logger error str
}

/**
  *
  * 未解决的问题:MapImplicitor
  */
case object TraversableImlicitor{

  def apply[T](t: Traversable[T]) = t match {
    case _ : Set[T] => SetImplicitor(t.toSet)
    case _ : Seq[T] => SeqImplicitor(t.toList)
    //TODO:MapImplicitor
  }
}
case class SetImplicitor[T](@transient set: Set[T])
  extends TraversableImlicitor[T]{
  override def sample (n: Int) = SeqImplicitor(set.toSeq) sample n toSet
}

case class SeqImplicitor[T](@transient seq : Seq[T])
  extends TraversableImlicitor[T]{
  /**
    * Seq的sample方法性能较高,所以使用的时候需要可
    * 能的使用Seq的sample方法,以消除多余的转换遍历
    * @param n
    * @return
    */
  override def sample (n: Int) = {
    if(seq.size / n == 0) seq
    else{
      val groupSize = seq.size / n + (if(seq.size % n > n / 2) 1 else 0)
      val b = Seq.newBuilder[T]
      var i = 0
      while( i < n ){
        b += seq( i * groupSize + random * (min(seq.size , (i + 1) * groupSize) - i * groupSize) toInt  )
        i += 1
      }
      b.result
    }
  }
}

case class MapImplicitor[T,U](@transient map : Map[T,U])
  extends TraversableImlicitor[(T,U)]{
  override def sample (n: Int) = SetImplicitor(map.keySet) sample n map(x => (x, map(x))) toMap
}
trait TraversableImlicitor[T] {
  def sample() : Traversable[T] = sample(20)
  def sample(n : Int): Traversable[T]
}

case class ArrayImplicitor[T](@transient array : Array[T]){
  /**
    * 针对参数解析
    * @tparam U
    * @return
    */
  def as[U <: Arguments](implicit tag: ClassTag[U]) =
    ArgumentsParser[U](array map(_ toString))


}
