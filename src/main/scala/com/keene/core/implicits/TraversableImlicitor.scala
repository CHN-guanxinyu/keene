package com.keene.core.implicits

import scala.util.Random

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
    * 尽可能的使用Seq的sample方法,以消除多余的转换遍历
    * @param n
    * @return
    */
  override def sample (n: Int) = {
    if(seq.size / n == 0) seq
    else{
      val groupSize = seq.size / n + (if(seq.size % n > n / 2) 1 else 0)
      val rand = new Random
      val b = Seq.newBuilder[T]
      var i = 0
      while( i < n ){
        b += seq( i * groupSize + rand.nextInt(Math.min(seq.size , (i + 1) * groupSize) - i * groupSize) )
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
