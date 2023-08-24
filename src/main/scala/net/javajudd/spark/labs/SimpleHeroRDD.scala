package net.javajudd.spark.labs

import org.apache.spark.sql.SparkSession

object SimpleHeroRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SimpleHeroRDD")
      .getOrCreate()

    val data = Seq(("Hulk", true), ("Groot", true), ("Thanos", true), ("Gamora", true), ("Hela", false), ("Ultron", false), ("Black Panther", true))
    val rdd = spark.sparkContext.parallelize(data)

    println("Data:")
    rdd.foreach(println)

    println(s"count: ${rdd.count()}")

    rdd.saveAsTextFile("/tmp/heros")
  }
}
