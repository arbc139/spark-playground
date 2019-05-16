package com.totorody.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.{DateTime, DateTimeConstants}
import org.joda.time.format.DateTimeFormat

object SundayCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException("Invalid argument numbers")
    }

    val filePath = args(0)
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    try {
      val textRDD = sc.textFile(filePath)

      val dateTimeRDD = textRDD.map { dateStr =>
        val pattern = DateTimeFormat.forPattern("yyyyMMdd")
        DateTime.parse(dateStr, pattern)
      }

      val sundayRDD = dateTimeRDD.filter { dateTime =>
        dateTime.getDayOfWeek == DateTimeConstants.SUNDAY
      }

      val numOfSunday = sundayRDD.count()
      println(s"=====================# of Sunday: ${numOfSunday}=====================")
    } finally {
      sc.stop()
    }
  }
}
