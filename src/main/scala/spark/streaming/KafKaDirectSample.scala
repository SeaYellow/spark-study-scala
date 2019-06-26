package spark.streaming

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafKaDirectSample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("KafKaDirectSample").master("local").getOrCreate()
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "fetch.message.max.bytes" -> "67108864",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val topics = Array("topicA", "topicB")

    val kafkaDS = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    kafkaDS.print()


    ssc.start()
    ssc.awaitTermination()
  }
}
