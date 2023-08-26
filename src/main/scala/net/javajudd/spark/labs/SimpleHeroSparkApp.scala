package net.javajudd.spark.labs

import org.apache.spark.sql.SparkSession

object SimpleHeroSparkApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleHeroRDD")
      .getOrCreate()

    val data = Seq(("Hulk", true), ("Groot", true), ("Thanos", true), ("Gamora", true), ("Hela", false), ("Ultron", false), ("Black Panther", true))
    val rdd = spark.sparkContext.parallelize(data)

    println("Data:")
    rdd.foreach(println)

    println(s"count: ${rdd.count()}")

    val sortedRdd = rdd.sortBy(_._1)
    //val sortedRdd = rdd.sortBy(_._1).collect() // should not be used with large result sets
    //val sortedRdd = rdd.sortBy(_._1,numPartitions = 1)
    println("Sorted:")
    sortedRdd.foreach(println)

    val groupByRdd = rdd.groupBy(_._2)
    groupByRdd.foreach(println)

    sortedRdd.saveAsTextFile("/tmp/heros")
  }
}
