package com.core.util

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object Utils {
  def main(args: Array[String]): Unit = {
    val line = "hello , Tom . 123"
    val pattern = new Regex("[a-zA-Z]")
    val matched = pattern.findAllMatchIn(line)
    val chars = new ArrayBuffer[String]
    for (m <- matched) {
      chars += m.group(0)
    }

    println(chars)
  }
}
