package spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TransformBlacklist {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformBlacklist").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val ssc = new StreamingContext(sc, Seconds(5))

    val blacklist = Array(Tuple2("Bob", true))

    val blacklistRDD = sc.parallelize(blacklist)

    val socketDS = ssc.socketTextStream("localhost", 9999)

    // 输入数据格式为 userId,userName;userId,userName
    val socketLogDS = socketDS.flatMap(line => line.split(";")).map(per => (per.split(",")(1), per))

    val filterDS = socketLogDS.transform(logRDD => {
      val leftJoinRDD = logRDD.leftOuterJoin(blacklistRDD)
      leftJoinRDD.filter(joinData => {
        if (joinData._2._2.getOrElse(false)) {
          false
        } else {
          true
        }
      })
    })

    filterDS.map(line => line._2._1).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
