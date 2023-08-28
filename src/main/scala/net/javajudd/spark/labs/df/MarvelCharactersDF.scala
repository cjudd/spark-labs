package net.javajudd.spark.labs.df

import org.apache.spark.sql.SparkSession

case class Character(id: Int, name: String, alignment: String, race: String, height: Double, flight: Boolean)

object MarvelCharactersRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Marvel Characters DataFrame")
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
    val raceDF = spark.sql("SELECT distinct(race) FROM characters")
    raceDF
      .repartition(1)
      .write
      .option("header", true)
      .csv(outputDir + "/race")
    raceDF.show(10)

    println("Heroes")
    val heroDF = spark.sql("SELECT Name FROM characters where Alignment_x = 'good'")
    heroDF
      .repartition(1)
      .write
      .option("header", true)
      .csv(outputDir + "/hero")
    heroDF.show(10)

    println("Flight")
    val flightDF = spark.sql("SELECT Name FROM characters where Flight = 'True'")
    flightDF
      .repartition(1)
      .write
      .option("header", true)
      .csv(outputDir + "/flight")
    flightDF.show(10)

  }
}
