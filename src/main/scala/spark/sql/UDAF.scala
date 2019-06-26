package spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object UDAF {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UDF").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val names = Array("Tom", "Tom", "Tom", "Bob", "Bob", "Jack", "Marry")
    val namesRDD = sc.parallelize(names)
    val namesRowRDD = namesRDD.map(name => Row(name))
    val structType = StructType(Array(StructField("name", StringType, true)))

    val namesDF = sqlContext.createDataFrame(namesRowRDD, structType)

    namesDF.registerTempTable("names")

    sqlContext.udf.register("nameCount", new StringCount)

    val resDF = sqlContext.sql("SELECT name,nameCount(name) AS cnt FROM names GROUP BY name ORDER BY cnt DESC")
    resDF.show()
  }
}
