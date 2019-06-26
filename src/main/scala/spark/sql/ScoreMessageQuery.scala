package spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object ScoreMessageQuery {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ScoreMessageQuery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val studentsDF = sqlContext.read.json("/Users/huanghai/Downloads/students.json")
    val scoresDF = sqlContext.read.json("/Users/huanghai/Downloads/scores.json")

    val joinDF = studentsDF.join(scoresDF, "id")

    joinDF.registerTempTable("students")

    joinDF.write.saveAsTable("students_score")

    val resDF = sqlContext.sql("SELECT * FROM students WHERE score > 80")

    resDF.foreach(row => println(row.getAs[Long]("id") + "," + row.getAs[String]("name") + "," + row.getAs[Long]("age") + "," + row.getAs[Long]("score")))

    resDF.write.json("/Users/huanghai/Downloads/queryResult.json")
  }
}
