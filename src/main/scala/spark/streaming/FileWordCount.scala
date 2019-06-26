package spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 监控目录
  */
object FileWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FileWordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val fileDS = ssc.textFileStream("/Users/huanghai/Downloads/streaming")
    fileDS.flatMap(line => line.split(",")).map((_, 1)).reduceByKey(_ + _).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
