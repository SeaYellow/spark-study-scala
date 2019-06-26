package spark.core.topn

import org.apache.spark.{SparkConf, SparkContext}

object TopN {
  def main(args: Array[String]): Unit = {
    val scores = Array(Tuple2("class1", 90), Tuple2("class1", 99), Tuple2("class2", 80), Tuple2("class2", 89), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class3", 90), Tuple2("class3", 100), Tuple2("class2", 60))
    val conf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreRDD = sc.parallelize(scores).cache()

    scoreRDD.sortBy(score => score._2, false).take(2).foreach(topN => println(topN._1 + "=" + topN._2))

  }
}
