package com.totorody.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingExample2 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("StreamingExample2")
    val ssc = new StreamingContext(conf, Seconds(10))

    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
