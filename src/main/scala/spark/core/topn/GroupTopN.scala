package spark.core.topn

import org.apache.spark.{SparkConf, SparkContext}

object GroupTopN {
  def main(args: Array[String]): Unit = {
    val scores = Array(Tuple2("class1", 90), Tuple2("class1", 99), Tuple2("class2", 80), Tuple2("class2", 89), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class3", 90), Tuple2("class3", 100), Tuple2("class2", 60))
    val conf = new SparkConf().setAppName("TopN").setMaster("local")
    val sc = new SparkContext(conf)

    val scoreRDD = sc.parallelize(scores)

    scoreRDD.groupByKey().map(scores => (scores._1, scores._2.toArray.sortWith(_ > _).take(2))).foreach(top => {
      println(top._1)
      top._2.foreach(println)
    })
  }
}
