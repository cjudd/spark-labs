package net.javajudd.spark.labs.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreamerDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("WordCountDFStream")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    import spark.implicits._
    val log = df.as[String]
      .filter(_.startsWith("ERROR"))
      .map(_.split(" "))
      .map(s => s"${s.tail.mkString(" ")}")

    log.writeStream
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .format("csv")
      .option("path", "/tmp/logstream")
      .option("checkpointLocation", "/tmp/checkpoint")
      .start()
      .awaitTermination()
  }

}
