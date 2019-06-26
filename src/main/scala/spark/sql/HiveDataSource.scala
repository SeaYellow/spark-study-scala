package spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("HiveDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    // 删除表，如果表存在
    hiveContext.sql("DROP TABLE IF EXISTS student_info")
    // 创建表，如果表不存在
    hiveContext.sql("CREATE TABLE IF NOT EXISTS  student_info(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ','")

    hiveContext.sql("LOAD DATA LOCAL INPATH '/data/study/data/students.txt' INTO TABLE student_info")


    // 查询表中数据
    val adultStudentsRDD = hiveContext.sql("SELECT * FROM student_info WHERE age >= 18")

    adultStudentsRDD.show()

    adultStudentsRDD.write.saveAsTable("adult_students")
  }
}
