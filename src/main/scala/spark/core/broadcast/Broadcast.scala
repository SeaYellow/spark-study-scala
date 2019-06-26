package spark.core.broadcast

import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {
  def main(args: Array[String]): Unit = {
    broadcast()
    accumulator()
  }


  def broadcast() = {
    val factor = 3
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Broadcast").setMaster("local")
    val sc = new SparkContext(conf)
    val broadcast = sc.broadcast(factor)
    val numbersRDD = sc.parallelize(numbers)
    numbersRDD.map(n => n * broadcast.value).foreach(println)
  }

  def accumulator() = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    val accumulator = sc.accumulator(0)

    val numberRDD = sc.parallelize(numbers)
    numberRDD.foreach(_ => accumulator += 1)

    println(accumulator)
  }
}
