package com.totorody.spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Random

object QuestionNaireDataGenerator {
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Must more than 1 arguments...")
    val qnFilePath = args(0)

    val conf = new SparkConf().setAppName("QuestionNaireDataGenerator")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    try {
      val schema = new StructType()
        .add(StructField("age", IntegerType))
        .add(StructField("gender", StringType))
        .add(StructField("score", IntegerType))

      val randomProvider = new Random(System.currentTimeMillis())

      val qnEntries = sc.parallelize(1 to 1000)
        .map(_ => {
          val gender = if (randomProvider.nextInt(2) == 1) "M" else "F"
          Row(
            randomProvider.nextInt(90),
            gender,
            randomProvider.nextInt(5)
          )
        })
      spark.createDataFrame(qnEntries, schema)
        .write
        .mode("Overwrite")
        .option("header", "true")
        .csv(qnFilePath)
    } finally {
      sc.stop()
    }
  }
}
