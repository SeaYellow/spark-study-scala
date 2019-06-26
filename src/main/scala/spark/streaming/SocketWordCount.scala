package spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * nc -lk 9999
  */
object SocketWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Streaming Word Count")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))


    val lines = ssc.socketTextStream("localhost", 9999)
    val res = lines.flatMap(line => line.split(",")).map((_, 1)).reduceByKey(_ + _)

    res.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
