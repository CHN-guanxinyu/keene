package com.keene.core.parsers

trait Parser[T] {
  def parse : T
}
