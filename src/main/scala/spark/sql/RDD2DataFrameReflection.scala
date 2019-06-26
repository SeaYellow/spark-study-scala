package spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RDD2DataFrameReflection extends App {
  val localJson = "/Users/huanghai/Downloads/students.txt"

  val conf = new SparkConf().setAppName("RDD to DataFrame").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = new SQLContext(sc)

  val studentsRDD = sc.textFile(localJson)

  // 引入隐式转换
  import sqlContext.implicits._

  case class Student(id: Int, name: String, age: Int)

  val studentDF = studentsRDD.map(_.split(",")).map(arr => Student(arr(0).toInt, arr(1), arr(2).toInt)).toDF()

  studentDF.write.parquet("/Users/huanghai/Downloads/students.parquet")


  // 注册成表
  studentDF.registerTempTable("students")

  val sqlDF = sqlContext.sql("SELECT * FROM students WHERE age > 18")

  sqlDF.show()

  val students = sqlDF.map(row => Student(row.getAs[Int]("id"), row.getAs("name"), row.getAs[Int]("age")))

  students.foreach(student => println(student.name + "=" + student.age + "=" + student.id))
}
