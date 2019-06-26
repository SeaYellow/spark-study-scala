package spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DailyUV {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DailyUV").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    val userLog = Array("2018-10-19,1276", "2018-10-19,1276", "2018-10-19,1276", "2018-10-19,1275", "2018-10-19,1270", "2018-10-19,1270", "2018-10-19,1270", "2018-10-20,1276", "2018-10-20,1276")

    val userRDD = sc.parallelize(userLog)

    val userRowRDD = userRDD.map(line => {
      val log = line.split(",")
      Row(log(0), log(1).toInt)
    })

    // 构建元数据信息
    val structType = StructType(Array(StructField("date", StringType, true), StructField("userid", IntegerType, true)))

    val userLogDF = sqlContext.createDataFrame(userRowRDD, structType)

    userLogDF.show()

    val resDF = userLogDF.groupBy("date").agg('date, countDistinct('userid))
    resDF.collect.foreach(row => println(row(1) + "=" + row(2)))
  }
}
