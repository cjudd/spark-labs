package net.javajudd.spark.labs.df

import org.apache.spark.sql.SparkSession

case class Character(id: Int, name: String, alignment: String, race: String, height: Double, flight: Boolean)

object MarvelCharactersRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Marvel Characters RDD")
      .getOrCreate()

    val inputFile = args(0)
    val outputDir = args(1)

    val characterDF = spark.read
      .options(Map(
        "header"->"true"
      ))
      .csv(inputFile)

    characterDF.printSchema()
    characterDF.show(1)

    characterDF.createOrReplaceTempView("characters")

    println("Races")
    spark.sql("SELECT distinct(race) FROM characters")
      .show(10)

    println("Heroes")
    spark.sql("SELECT Name FROM characters where Alignment_x = 'good'")
      .show(10)

    println("Flight")
    spark.sql("SELECT Name FROM characters where Flight = 'True'")
      .show(10)
  }
}
