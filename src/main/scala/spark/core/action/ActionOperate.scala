package spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionOperate {
  def main(args: Array[String]): Unit = {
    //    reduce()
    //    collect()
    //    count()
    //    take()

    countByKey()
  }


  def reduce(): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Reduce").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(numbers)

    val res = numbersRDD.reduce(_ + _)

    println(res)
  }


  def collect(): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Collect").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(numbers)

    val res = numbersRDD.collect()

    println(res.mkString(","))
  }


  def take(): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Take").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(numbers)

    val res = numbersRDD.take(3)

    println(res.mkString(","))
  }

  def count(): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Count").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(numbers)

    val res = numbersRDD.count()

    println(res)
  }


  def countByKey(): Unit = {
    val scores = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class2", 60))
    val conf = new SparkConf().setAppName("CountByKey").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(scores)

    numbersRDD.countByKey().foreach(c => println("class is : " + c._1 + " number : " + c._2))
  }
}
