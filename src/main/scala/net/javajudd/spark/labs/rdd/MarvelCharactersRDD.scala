package net.javajudd.spark.labs.rdd

import org.apache.spark.sql.SparkSession

case class Character(id: Int, name: String, alignment: String, race: String, height: Double, flight: Boolean)

object MarvelCharactersRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Marvel Characters RDD")
      .getOrCreate()

    val inputFile = args(0)
    val outputDir = args(1)

    val csvRdd = spark.sparkContext.textFile(inputFile)
    val characterRdd = csvRdd
      .filter(!_.startsWith(",ID,Name,Alignment_x,"))
      .map(row => {
        val fields = row.split(",").map(_.trim)
        val flight = if(fields.length >= 27) {
          fields(27).toBoolean
        } else { false }
        Character(fields(1).toInt, fields(2), fields(3), fields(6), fields(10).toDouble, flight)
      })

    // list distinct races
    characterRdd
      .map(c => c.race)
      .distinct()
      .sortBy(r => r)
      .saveAsTextFile(outputDir + "/race")

    // list heroes
    characterRdd
      .filter(_.alignment == "good")
      .map(c => c.name)
      .sortBy(n => n)
      .saveAsTextFile(outputDir + "/heros")

    // list heroes with flight
    characterRdd
      .filter(_.flight)
      .map(c => c.name)
      .sortBy(n => n)
      .saveAsTextFile(outputDir + "/flight")
  }
}
