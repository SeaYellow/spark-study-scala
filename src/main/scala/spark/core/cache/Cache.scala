package spark.core.cache

import org.apache.spark.{SparkConf, SparkContext}

object Cache {
  def main(args: Array[String]): Unit = {
    val localFile = "/Users/huanghai/Data/Bigdata/spark-1.6.0-bin-hadoop2.6/README.md"

    val conf = new SparkConf().setAppName("Cache").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile(localFile)

    val start = System.currentTimeMillis()
    val count = dataRDD.count()
    dataRDD.count()
    println("Count : " + count + " Total Use time : " + (System.currentTimeMillis() - start))
  }
}
