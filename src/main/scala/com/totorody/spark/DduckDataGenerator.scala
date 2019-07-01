package com.totorody.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import scala.util.Random

object DduckDataGenerator {
  val timeFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    require(args.length >= 2, "Must more than 2 arguments...")
    val octoberFilePath = args(0)
    val novemberFilePath = args(1)

    val conf = new SparkConf().setAppName("DduckDataGenerator")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    try {
      val schema = new StructType()
        .add(StructField("id", IntegerType))
        .add(StructField("time", StringType))
        .add(StructField("product", IntegerType))
        .add(StructField("sold", IntegerType))

      val randomProvider = new Random(10)
      val start = DateTime.parse(
        "2019-10-01 00:00:00",
        timeFormat
      )

      // october - 5000 to 5999
      val octoberIds = sc.parallelize(5000 to 5999)
      val octoberEntries = octoberIds
        .map(
          id => {
            Row(
              id,
              timeFormat.print(start.plusSeconds(id)),
              randomProvider.nextInt(20),
              randomProvider.nextInt(100)
            )
          }
        )
      spark.createDataFrame(octoberEntries, schema)
        .write
        .mode("Overwrite")
        .option("header", "true")
        .csv(octoberFilePath)

      // novemeber - 6000 to 6999
      val novemberIds = sc.parallelize(6000 to 6999)
      val novemberEntries = novemberIds
        .map(
          id => {
            Row(
              id,
              timeFormat.print(start.plusSeconds(id)),
              randomProvider.nextInt(20),
              randomProvider.nextInt(100)
            )
          }
        )
      spark.createDataFrame(novemberEntries, schema)
        .write
        .mode("Overwrite")
        .option("header", "true")
        .csv(novemberFilePath)
    } finally {
      sc.stop()
    }
  }
}
