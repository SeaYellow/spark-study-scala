package spark.streaming

import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * nc -lk 9999
  */
object WindowWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WindowWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val socketDS = ssc.socketTextStream("localhost", 9999)

    val windowDS = socketDS.map(line => (line.split(" ")(1), 1)).reduceByKeyAndWindow((v1: Int, v2: Int) => v1 + v2, Duration(50000), Duration(10000))

    val resDS = windowDS.transform(searchRDD => {
      val reverseRDD = searchRDD.map(tuple => (tuple._2, tuple._1))
      reverseRDD.sortByKey(false)
    })

    resDS.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
