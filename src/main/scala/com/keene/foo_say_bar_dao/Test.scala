package com.keene.foo_say_bar_dao

import java.io.File
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.text.SimpleDateFormat
import java.util
import java.util.Calendar
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{Executors, TimeUnit}

import breeze.io.RandomAccessFile
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
import org.apache.spark.sql.SparkSession

import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.ForkJoinTaskSupport
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.Random
import scala.xml.Node

object Test9 extends App {
  override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
    results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
          remainingBlocks.isEmpty))
  }
}

class StringWraper(str : String) {
  val _str : String = str
}

object Test8 extends App with SimpleSpark[SparkSession] {
  val file = new File("D:\\tmp\\idea\\log.txt")
  val channel : FileChannel = new RandomAccessFile(file, "r").getChannel
  channel.map(MapMode.READ_ONLY, 0, file.length)
  val buf : ByteBuffer = ByteBuffer.allocate(file.length.toInt)
  channel.read(buf)
  println(file.length)
  val txt = new StringWraper(new String(buf.array()))
  println(txt)
  Thread sleep 99999999
}

object Test7 extends App {
  val l = Iterator(1, 2, 3, 4, 5)

  def f(i : Int) = {
    println("f")
    i
  }

  def g(i : Int) = {
    println("g")
    i
  }

  def h(it : Iterator[Int]) = {
    println("h")
    Iterator(it.sum)
  }

  h(l map f) map g sum
}

object Test6 extends App {

  class BlockQueue[U](capacity : Int) {
    val _lock = new ReentrantLock
    val _empty = _lock.newCondition
    val _full = _lock.newCondition
    val _queue = new mutable.Queue[U]()

    def lock = _lock.lock

    def unlock = _lock.unlock

    def full = _queue.size == capacity

    def empty = _queue.isEmpty

    def put(e : U) : Unit = {
      lock
      while (full) _full.await
      _queue enqueue e
      _empty.signalAll
      unlock
    }

    def get() : U = {
      lock
      while (empty) _empty.await
      val r = _queue.dequeue
      _full.signalAll
      unlock
      r
    }
  }

  val q = new BlockQueue[Int](10)
  val pool = Executors.newFixedThreadPool(8)

  val t = new Thread(new Runnable {
    override def run() : Unit = {
      while (true) {
        q put Random.nextInt(10)
      }
    }
  })
  t setName "put-thread"
  t start

  while (true) {
    q.get
  }


}

object Test5 extends App with SimpleSpark[Any] {
  val a = sc.range(0, 10000)
  val b = Array.fill(100)(a).reduce(_ ++ _).map(_ -> 1).reduceByKey(_ + _, 2000).map(_._1).collect
}

object Test4 extends App with SimpleSpark[Any] {
  (1 to 5) foreach { _ => {
    val a = sc.range(0, 10e8.toLong)
    println(a.getNumPartitions)

    def now = System.currentTimeMillis

    val start = now
    a.max
    println(now - start)
  }
  }
}

object Test3 extends App {
  val capacity = 4
  val map = new util.LinkedHashMap[Int, Int](capacity, 0.75f, true) {
    override protected def removeEldestEntry(eldest : util.Map.Entry[Int, Int]) : Boolean =
      size > capacity
  }

  def f(
    k : Int,
    v : Int
  ) = map.put(k, v)

  f(1, 1)
  f(2, 1)
  f(3, 1)
  f(4, 1)
  f(2, 1)
  f(1, 1)
  f(5, 1)
  f(6, 1)
  f(1, 1)
  f(8, 1)
  f(1, 1)

  map.productIterator foreach println

}

object Test2 extends App with SimpleSpark[SparkSession] {
  spark.fromKafka("kafka-broker2.jd.local:9092", "kafka-broker2.jd.local:9092").toConsole
  spark.streams.awaitAnyTermination
}

object Task extends App with SimpleSpark[SparkSession] {


  def pairs(start : String = "2018-08-13 00:00:00") = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cld = Calendar.getInstance
    cld setTime fmt.parse(start)

    val res = ListBuffer.newBuilder[Long]
    val now = fmt.parse("2018-08-14 00:00:00")

    while (now after cld.getTime) {
      res += cld.getTime.getTime
      cld.add(Calendar.HOUR_OF_DAY, 1)
    }

    res += cld.getTime.getTime

    val result = res.result.toList
    result.indices zip (0l :: result zip result drop 1)
  }

  val ses = pairs().par

  ses.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(6))

  val a = ses.map { case (h, (start, end)) => (h, spark.sql(
    s"""
       |select count(1)
       |from antidb.search_app_click_log
       |where dt = '2018-08-08'
       |  and (event_id = 'Searchlist_Productid'
       |  or event_id = 'SearchList_Productid'
       |  or event_id = 'Searchlist_AddtoCartforfood')
       |  and page_param != '搜索:6240_6233_list' and page_param != '搜索:_0'
       |  and split(page_param, ':')[0] = '搜索' and size(split(page_param, '_')) >= 4 and size(split(event_param, '_')) >= 5
       |  and split(event_param, '_')[0] is not null and length(split(event_param, '_')[0]) > 0
       |  and size(split(page_param, ':')) > 1
       |  and click_ts >= $start and click_ts < $end
      """.stripMargin).count)
  }.toList

  val tre = 15000000l
  var total = 0l
  var i = 0
  ("" /: a) { case (n, (h, c)) =>
    val next = if (total > tre) {

      total = 0
      "@" + h
    } else {
      "_" + h
    }
    total += c

    println(i, total)
    i += 1
    n + next
  }

  /*val sqls = Map(//    "catfish_free_commission_spam_order" -> List("cps", "gdt", "jzt", "tpm"),
    //    "base_ads_click_sum_log" -> List("cps", "gdt", "jzt", "tpm"),
    //    "base_ads_order_sum_log" -> List("cps", "gdt", "jzt", "tpm")
    "base_user_behaviour_sum_log" -> List("app", "pc_m", "wx_sq")).mapValues { pts => for (pt <- pts; dt <- toNow()) yield (pt, dt) }.flatMap { case (table, partitions) => partitions map { case (pt, dt) => s"""|load data inpath '/user/jd_ad/ads_anti/guanxinyu/metadata/hive/$table/pt=$pt/dt=$dt'|overwrite into table antidb.$table|partition(pt='$pt', dt='$dt')
         """.stripMargin
  }
  }.par

  sqls.tasksupport = new ForkJoinTaskSupport(new ForkJoinPool(4))

  sqls map spark.sql*/
}

sealed trait AttributeSupport {
  def node : Map[String, String]

  protected def _key(k : String = "") = key = k

  private var key : String = _
  protected lazy val value = node(key)

}

sealed trait OptionalAttributeSupport extends AttributeSupport {
  def isDefined = false

  def doThis

  if (isDefined) doThis
}

trait FromSupport extends AttributeSupport {
  _key("from")
  val from = value
}

trait UnionSupport extends OptionalAttributeSupport {
  _key("union-table")
  val unionTable = value

  override def doThis : Unit = println(1)
}

class A extends UnionSupport with FromSupport {

  def node = Map("from" -> "123", "union-table" -> "qwe")

}

object Task1 extends App with SimpleSpark[Any] {
  val a = new A()
  println(a.from, a.unionTable)
}

