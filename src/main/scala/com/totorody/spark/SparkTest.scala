package com.totorody.spark

import org.apache.spark.sql.SparkSession

object SparkTest {
  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkTest").getOrCreate()
    println("Hello Spark")
  }
}
