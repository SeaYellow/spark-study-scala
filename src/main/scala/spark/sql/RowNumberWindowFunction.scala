package spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object RowNumberWindowFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RowNumberWindowFunction").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val data = Array("apple5,4500", "apple5,7500", "apple5,4550", "apple5,5500", "apple5 plus,5550", "apple5 plus,6500", "apple5 plus,5500", "apple6,6500", "apple6,6000", "apple6 plus,6500")

    val dataRDD = sc.parallelize(data)
    val rowRDD = dataRDD.map(line => {
      val lines = line.split(",")
      Row(lines(0), lines(1).toInt)
    })

    // 设置元数据信息
    val structType = StructType(Array(StructField("phoneType", StringType, true), StructField("price", IntegerType, true)))
    val dataDF = sqlContext.createDataFrame(rowRDD, structType)
    dataDF.registerTempTable("sales")
    val top3DF = sqlContext.sql("SELECT t.phoneType,t.price FROM (SELECT phoneType,price,row_number() OVER (ORDER BY price DESC) AS rank FROM sales) t WHERE t.rank < 3")
    val groupTop3DF = sqlContext.sql("SELECT t.phoneType,t.price FROM (SELECT phoneType,price,row_number() OVER (PARTITION BY phoneType ORDER BY price DESC) AS rank FROM sales) t WHERE t.rank < 3")
    top3DF.show()
    groupTop3DF.show()
  }
}