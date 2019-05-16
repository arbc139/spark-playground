package com.totorody.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Must more than 1 arguments...")

    val conf = new SparkConf()
      .setAppName("WordCount")
    val sc = new SparkContext(conf)

    try {
      val filePath = args(0)
      val wordCountRDD = sc.textFile(filePath)
          .flatMap(_.split("[ ,.]"))
          .filter(_.matches("""\p{Alnum}+"""))
          .map((_, 1))
          .reduceByKey(_ + _)
      val top3WordCountRDD = wordCountRDD.map { case(word, count) => (count, word) }
        .sortByKey(false)
        .map { case(word, count) => (count, word) }
      top3WordCountRDD.take(3).foreach(println)
    } finally {
      sc.stop()
    }
  }
}
