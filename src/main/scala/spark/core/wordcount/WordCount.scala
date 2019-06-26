package spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val localFile = "/Users/huanghai/Data/Bigdata/spark-1.6.0-bin-hadoop2.6/README.md"

    val hdfsFile = "hdfs://bigdata:9000/data/wordcount.txt"

    val conf = new SparkConf().setAppName("SocketWordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val lines = sc.textFile(localFile)

    lines.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).sortBy(_._2, false).foreach(res => println(res._1 + " = " + res._2))
  }

}
