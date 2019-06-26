package spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by HUANGHAI on 2019/6/20.
  */
object ParquetWriterSample {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetWriterTest").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val data = sc.textFile("E:\\idea_workspace\\spark-study-scala\\data\\data.txt")

    val schema = StructType(Array(StructField("name", StringType, true), StructField("age", IntegerType, true)))

    val rdd = data.map(_.split(",")).map(r => Row(r(0), r(1).toInt))

    val df = sqlContext.createDataFrame(rdd, schema)

    df.write.parquet("E:\\idea_workspace\\spark-study-scala\\data\\parquet")

    sc.stop()
  }

}
