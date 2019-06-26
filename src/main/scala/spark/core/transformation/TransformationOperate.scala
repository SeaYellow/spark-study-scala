package spark.core.transformation

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOperate {
  def main(args: Array[String]): Unit = {
    //    sortByKey()
    //    join()
    coGroup()
  }

  def filter(): Unit = {
    val numbers = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val conf = new SparkConf().setAppName("Filter").setMaster("local")
    val sc = new SparkContext(conf)
    val numbersRDD = sc.parallelize(numbers)
    numbersRDD.filter(_ % 2 == 0).foreach(println)
  }


  def groupByKey(): Unit = {
    val conf = new SparkConf().setAppName("Group by key").setMaster("local")
    val sc = new SparkContext(conf)

    val scores = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class2", 60))

    val scoresRDD = sc.parallelize(scores)

    scoresRDD.groupByKey().foreach(score => {
      println(score._1);
      score._2.foreach(singleScore => println(singleScore))
    })
  }


  def reduceByKey(): Unit = {
    val conf = new SparkConf().setAppName("Reduce by key").setMaster("local")
    val sc = new SparkContext(conf)

    val scores = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class2", 60))

    val scoresRDD = sc.parallelize(scores)

    scoresRDD.reduceByKey(_ + _).foreach(score => {
      println(score._1 + ",Total scores = " + score._2)
    })
  }


  def sortByKey(): Unit = {
    val conf = new SparkConf().setAppName("Sort by key").setMaster("local")
    val sc = new SparkContext(conf)

    val scores = Array(Tuple2("class1", 90), Tuple2("class2", 80), Tuple2("class1", 75), Tuple2("class3", 85), Tuple2("class2", 60))

    val scoresRDD = sc.parallelize(scores)

    scoresRDD.sortByKey().foreach(score => {
      println(score._1 + " = " + score._2)
    })
  }

  def join(): Unit = {
    val conf = new SparkConf().setAppName("Join").setMaster("local")
    val sc = new SparkContext(conf)

    val students = Array(Tuple2(1, "Tom"), Tuple2(2, "Bob"), Tuple2(3, "Jack"))
    val scores = Array(Tuple2(1, 90), Tuple2(2, 85), Tuple2(3, 60))

    val studentsRDD = sc.parallelize(students)
    val scoresRDD = sc.parallelize(scores)

    val joinRDD = studentsRDD.join(scoresRDD)

    joinRDD.foreach(record => {
      println("id : " + record._1)
      println("name : " + record._2._1)
      println("score : " + record._2._2)
    })
  }


  def coGroup(): Unit = {
    val conf = new SparkConf().setAppName("CoGroup").setMaster("local")
    val sc = new SparkContext(conf)

    val students = Array(Tuple2(1, "Tom"), Tuple2(2, "Bob"), Tuple2(3, "Jack"))
    val scores = Array(Tuple2(1, 90), Tuple2(2, 85), Tuple2(3, 60), Tuple2(1, 70))

    val studentsRDD = sc.parallelize(students)
    val scoresRDD = sc.parallelize(scores)

    val joinRDD = studentsRDD.cogroup(scoresRDD)

    joinRDD.foreach(record => {
      println("id : " + record._1)
      record._2._1.foreach(name => println("name : " + name))
      record._2._2.foreach(score => println("score : " + score))
    })
  }
}
