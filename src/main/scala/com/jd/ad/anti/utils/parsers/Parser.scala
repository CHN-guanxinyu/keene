package com.jd.ad.anti.utils.parsers

trait Parser[F , T] {
  def parse(raw : F) : T
}
