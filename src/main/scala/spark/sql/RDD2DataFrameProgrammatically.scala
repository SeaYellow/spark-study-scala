package spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}

object RDD2DataFrameProgrammatically extends App {
  val localFile = "/Users/huanghai/Downloads/students.txt"

  val conf = new SparkConf().setAppName("RDD to DataFrame").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val studentsRDD = sc.textFile(localFile)

  // 第一步，构建出元素为Row的普通RDD
  val rowRDD = studentsRDD.map(line => {
    val arr = line.split(",")
    Row(arr(0).toInt, arr(1), arr(2).toInt)
  })

  // 第二步，动态构建元数据
  val structType = StructType(Array(StructField("id", IntegerType, true), StructField("name", StringType, true), StructField("age", IntegerType, true)))

  // 第三步,将RDD转化为DataFrame
  val studentDF = sqlContext.createDataFrame(rowRDD, structType)

  studentDF.registerTempTable("students")

  val sql = sqlContext.sql("SELECT * FROM students WHERE age = 19")

  sql.foreach(row => println(row.getAs[Int]("id") + "," + row.getAs[String]("name") + "," + row.getAs[Int]("age")))

}
