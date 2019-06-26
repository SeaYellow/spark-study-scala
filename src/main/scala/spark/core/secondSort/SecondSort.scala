package spark.core.secondSort

import org.apache.spark.{SparkConf, SparkContext}

object SecondSort {
  def main(args: Array[String]): Unit = {
    val array = Array(Tuple2(1, 3), Tuple2(1, 2), Tuple2(2, 2), Tuple2(2, 3), Tuple2(1, 5), Tuple2(3, 1))

    val conf = new SparkConf().setAppName("Second Sort").setMaster("local")
    val sc = new SparkContext(conf)
    val arrayRDD = sc.parallelize(array)
    arrayRDD.map(t => (new SecondSortKey(t._1, t._2), t)).sortByKey().foreach(t => println(t._2._1 + "=" + t._2._2))
  }

}
