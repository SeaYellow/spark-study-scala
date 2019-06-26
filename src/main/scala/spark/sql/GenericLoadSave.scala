package spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object GenericLoadSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Load and Save").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val studentDF = sqlContext.read.parquet("/Users/huanghai/Downloads/students.parquet")

    studentDF.printSchema()

    studentDF.registerTempTable("students")

    val resDF = sqlContext.sql("SELECT * FROM students WHERE name = 'Tom'")

    resDF.foreach(row => println(row.getAs[Int]("id") + "," + row.getAs[String]("name") + "," + row.getAs[Int]("age")))


    resDF.write.mode(SaveMode.Overwrite).orc("/Users/huanghai/Downloads/students.orc")

    resDF.write.mode(SaveMode.Overwrite).json("/Users/huanghai/Downloads/students_save.json")

  }
}
