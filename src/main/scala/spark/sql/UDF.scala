package spark.sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object UDF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDF").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val names = Array("Tom", "Bob", "Jack", "Marry")
    val namesRDD = sc.parallelize(names)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))

    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.registerTempTable("names")

    sqlContext.udf.register("strLen", (name: String) => name.length)

    val resDF = sqlContext.sql("SELECT name,strLen(name) AS nameLength FROM names")
    resDF.show()
  }
}
