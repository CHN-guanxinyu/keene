package com.keene.core.implicits

import scala.util.Random

case class TraversableImlicitor[T](@transient t : Traversable[T]) {
  def sample() : Traversable[T] = sample(20)

  def sample(n : Int): Traversable[T] ={
    if(t.size / n == 0) t
    else{
      val groupSize = t.size / n + (if(t.size % n > n / 2) 1 else 0)
      val rand = new Random
      val b = Seq.newBuilder[T]
      var i = 0
      while( i < n ){
        b += t( i * groupSize + rand.nextInt(Math.min(t.size , (i + 1) * groupSize) - i * groupSize) )
        i += 1
      }
      b.result
    }
  }

  private def fastInitAndLast[T](it : Iterator[T])=
    ((List.empty[T], it.next ) /: it) {
      case ( (init, last), e ) => (last :: init, e)
    }

}
