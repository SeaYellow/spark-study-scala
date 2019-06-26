package spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Json2DataFrame {

  /**
    * {"id":1,"name":"Bob","age":18}
    * {"id":2,"name":"Tom","age":20}
    * {"id":3,"name":"Jack","age":19}
    */

  def main(args: Array[String]): Unit = {
    val localJson = "/Users/huanghai/Downloads/students.json"

    val conf = new SparkConf().setAppName("Json to DataFrame").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // 构建DataFrame
    val dataFrame = sqlContext.read.json(localJson)

    dataFrame.show()
    // 打印DataFrame元数据
    dataFrame.printSchema()
    // 查询某一列数据
    dataFrame.select(dataFrame.col("name"), dataFrame.col("age") + 1).show()
    // 对某一列数据进行过滤
    dataFrame.filter(dataFrame.col("age") > 18).show()
    // 对某一列进行分组统计
    dataFrame.groupBy(dataFrame.col("age")).count().show()
  }
}
