package net.javajudd.spark.labs.streaming

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogStreamer {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("LogStreamerRDD")

    val output = args(0)

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.socketTextStream("localhost", 9999)
      .filter(_.startsWith("ERROR"))
      .map(_.split(" "))
      .map(s => s"${s(1)},${s(2)}")
      .saveAsTextFiles(output)

    ssc.start()
    ssc.awaitTermination()
  }
}
