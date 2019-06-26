package spark.core.init_rdd

import org.apache.spark.{SparkConf, SparkContext}

object Array2RDD {
  def main(args: Array[String]): Unit = {
    val array = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Array RDD").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRdd = sc.parallelize(array)
    val res = dataRdd.reduce(_ + _)
    println(res)
  }
}
