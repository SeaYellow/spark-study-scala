package spark.sql

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object JDBCDataSource {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCDataSource").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val url = "jdbc:mysql://localhost:3306/test"
    val tableName = "student_infos"
    val options = new Properties()
    options.put("user", "root")
    options.put("password", "huanghai")
    options.put("driver", "com.mysql.jdbc.Driver")

    val studentsDF = sqlContext.read.jdbc(url, tableName, options)

    studentsDF.registerTempTable("student_infos")

    val adultStudentsDF = sqlContext.sql("SELECT * FROM student_infos WHERE age >=18")

    adultStudentsDF.show()

  }
}
