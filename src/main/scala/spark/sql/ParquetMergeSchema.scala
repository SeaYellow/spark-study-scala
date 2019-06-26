package spark.sql

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object ParquetMergeSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ParquetMergeSchema").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    //    sqlContext.setConf("spar.sql.parquet.mergeSchema", "true")

    val student = Array(Tuple2("Tom", 19), Tuple2("Bob", 18), Tuple2("Jack", 22))
    val studentRDD = sc.parallelize(student)

    import sqlContext.implicits._

    val studentDF = studentRDD.toDF("name", "age")
    studentDF.write.mode(SaveMode.Overwrite).parquet("/Users/huanghai/Downloads/students_merge.parquet")

    val scores = Array(Tuple2("Tom", 90), Tuple2("Bob", 100), Tuple2("Jack", 89))
    val scoresRDD = sc.parallelize(scores)
    val scoreDF = scoresRDD.toDF("name", "score")
    scoreDF.write.mode(SaveMode.Append).parquet("/Users/huanghai/Downloads/students_merge.parquet")

    val studentParseDF = sqlContext.read.option("mergeSchema", "true").parquet("/Users/huanghai/Downloads/students_merge.parquet")

    studentParseDF.printSchema()
    studentParseDF.show()
  }
}
