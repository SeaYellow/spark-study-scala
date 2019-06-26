package spark.core.charcount

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object CharCount {
  def main(args: Array[String]): Unit = {
    val localFile = "/Users/huanghai/Data/Bigdata/spark-1.6.0-bin-hadoop2.6/README.md"

    val conf = new SparkConf().setAppName("Char Count").setMaster("local")

    val sc = new SparkContext(conf)

    val rdd = sc.textFile(localFile)

    val pattern = new Regex("[a-zA-Z]")

    //    rdd.map(line => pattern.findAllMatchIn(line).mkString(",")).
    //      flatMap(_.split(",")).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).
    //      foreach(res => println(res._1 + " = " + res._2))
    rdd.flatMap(line => {
      val matched = pattern.findAllMatchIn(line)
      val chars = new ArrayBuffer[String]()
      for (m <- matched) {
        chars += m.group(0)
      }
      chars
    }).map((_, 1)).reduceByKey(_ + _).sortBy(_._2, false).foreach(res => println(res._1 + " = " + res._2))
  }
}
