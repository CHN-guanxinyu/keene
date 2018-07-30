package com.keene.core.parsers

trait Parser[F , T] {
  def parse(raw : F) : T
}
