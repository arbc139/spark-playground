package com.totorody.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object QuestionNaireSummary {
  def main(args: Array[String]): Unit = {
    require(args.length >= 1, "Must more than 1 arguments...")
    val qnFilePath = args(0)

    val conf = new SparkConf().setAppName("QuestionNaireSummary")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val schema = new StructType()
      .add(StructField("age", IntegerType))
      .add(StructField("gender", StringType))
      .add(StructField("score", IntegerType))

    try {
      val qn = spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .load(qnFilePath)

      qn.cache

      // All average
      val avgAll = qn.agg(avg("score").alias("score_avg"))
        .select("score_avg")

      // Average by age
      val avgAgeRDD = qn.toDF().rdd.map(row => Row(
        row.getAs[Int]("age") / 10 * 10,
        row.getAs[String]("gender"),
        row.getAs[Int]("score")
      ))
      val avgAge = spark.createDataFrame(avgAgeRDD, schema)
        .groupBy("age")
        .agg(avg("score").alias("score_avg"))
        .select("age", "score_avg")

      // Average by gender
      val avgGender = qn.groupBy("gender")
        .agg(avg("score").alias("score_avg"))
        .select("gender", "score_avg")

      avgAll.show()
      avgAge.show()
      avgGender.show()
    } finally {
      spark.close()
    }
  }
}
