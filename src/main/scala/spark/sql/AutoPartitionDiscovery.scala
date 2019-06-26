package spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 自动分区推断
  */
object AutoPartitionDiscovery {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AutoPartitionDiscovery").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)
    val studentsDF = sqlContext.read.orc("hdfs://bigdata:9000//students/gender=male/country=US/students.orc/part-r-00000-0f30c4a8-3b4d-4377-89b4-32da2feaa14a.orc")

    studentsDF.printSchema()
    studentsDF.show()
  }
}
