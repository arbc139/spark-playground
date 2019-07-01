package com.totorody.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object StreamingExample1 {
  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Must more than 2 arguments...")

    val host = args(0)
    val port = args(1)

    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("StreamingExample1")
    val ssc = new StreamingContext(conf, Seconds(10))
    val lines = ssc.socketTextStream(host, port.toInt, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(_.split(" ")).filter(_.nonEmpty)
    val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
