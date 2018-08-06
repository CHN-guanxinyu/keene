package com.keene.spark.examples.easy

import akka.actor._
import com.keene.spark.utils.SimpleSpark
import com.keene.core.implicits._
object Main extends SimpleSpark {
  implicit var s : String = "hi"
  def foo(implicit s : String) = s
  lazy val a : String = foo
  def main(args:Array[String]):Unit= {
    val s = ActorSystem("123")
    s.actorOf(Props[A1], "a1")
    println("done")
  }
}
case object Start
case object Success
class A1 extends Actor {
  var counter = 0


  val a2 = context.system.actorOf(Props[A2],"a2")
  val a3 = context.system.actorOf(Props[A2],"a3")
  self ! (Start, 1)
//  val a3 = context.system.actorOf(Props[A3],"a3")
  override def receive = {
    case (Start, n) =>
      println(n)
      a2 ! Start
      Thread sleep 20
      a3 ! Start
    case Success =>
      counter += 1
      if(counter == 2) {
        println("all finished")
        context.system.terminate
      }
  }
}
class A2 extends Actor with SimpleSpark {
  override def receive = {
    case Start =>
      println("a2 success")
      sender ! Success
  }
}