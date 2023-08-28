package net.javajudd.spark.labs

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.xml.XML

class XMLToCSV(@transient spark: SparkSession, inputPath: String, outputPath: String) extends Serializable {

  import spark.implicits._
  @transient val sc: SparkContext = spark.sparkContext

  def build(): Unit = {
    val outputRdd = sc.wholeTextFiles(inputPath + "*.xml")
      .map {
        // (filename, file content)
        case (k, v) =>
          val xml = XML.loadString(v)
          val uri = (xml \ "substance-uri").text
          val rn = (xml \ "substance-uri" \ "@registry-number").text
          val displayNameNode = (xml \ "display-name")
          val formula = (xml \ "molecular-formula" \ "formula").text
          val displayName = if(displayNameNode.isEmpty) "No Display Name" else displayNameNode.text
          val synonyms = (xml \ "substance-name" \ "name").map(x => x.text).toSet.mkString(";")
          (uri, rn, displayName, formula, synonyms)
      }

    val outputDF = outputRdd.toDF("Substance URI", "Registry Number", "Display Name", "Formula", "Synonyms")
    outputDF
      .repartition(numPartitions = 1)
      .write
      .option("header", "true")
      .csv(outputPath)
  }
}

object XMLToCSV {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("XML to CSV")
      .getOrCreate()

    var inputPath = ""
    var outputPath = ""

    args.sliding(2, 1).toList.collect {
      case Array("--input-path", argOutput: String) => inputPath = argOutput
      case Array("--output-path", argOutput: String) => outputPath = argOutput
    }

    val client = new XMLToCSV(spark, inputPath, outputPath)
      client.build()
  }
}
