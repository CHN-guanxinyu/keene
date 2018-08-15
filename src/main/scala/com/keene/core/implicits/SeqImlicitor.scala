package com.keene.core.implicits

import scala.util.Random

case class SeqImlicitor(@transient t : Seq[_]) {
  def sample(n : Int = 20): Seq[_] ={
    val groupSize = t.size / n
    if(groupSize == 0) t
    else{
      val it = t grouped groupSize
      val (init, last) = ((List[Seq[_]](), it.next() ) /: it) { case ( (init, last), e ) =>
        (last :: init, e)
      }
      val rand = new Random
      (init.par.map{ _(rand nextInt groupSize) } :+ last( rand nextInt last.size )) toList
    }
  }
}
