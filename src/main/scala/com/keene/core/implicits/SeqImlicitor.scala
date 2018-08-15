package com.keene.core.implicits

import scala.util.Random

case class SeqImlicitor(@transient t : Seq[_]) {
  def sample = sample(20)

  def sample(n : Int): Seq[_] ={
    val groupSize = t.size / n
    if(groupSize == 0) t
    else{
      val it = t grouped groupSize
      val (init, last) = fastInitAndLast(it)
      val rand = new Random
      (init.par.map{ _(rand nextInt groupSize) } :+ last( rand nextInt last.size )) toList
    }
  }

  private def fastInitAndLast[T](it : Iterator[T])=
    ((List.empty[T], it.next ) /: it) {
      case ( (init, last), e ) => (last :: init, e)
    }

}
