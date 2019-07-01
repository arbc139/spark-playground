package com.totorody.spark

import java.io.{BufferedReader, InputStreamReader}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._

object DduckCount {
  def main(args: Array[String]): Unit = {
    require(args.length >= 3, "Must more than 3 arguments...")
    val octFilePath = args(0)
    val novFilePath = args(1)
    val productFilePath = args(2)

    val conf = new SparkConf().setAppName("DduckCount")
    val spark = SparkSession.builder.config(conf).getOrCreate()

    val schema = new StructType()
      .add(StructField("id", IntegerType))
      .add(StructField("time", StringType))
      .add(StructField("product", IntegerType))
      .add(StructField("sold", IntegerType))

    try {
      val oct = spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .load(octFilePath)
      val nov = spark.read.format("csv")
        .schema(schema)
        .option("header", "true")
        .load(novFilePath)
      val product = spark.read.format("csv")
        .option("header", "true")
        .load(productFilePath)

//      val productsMap = new util.HashMap[String, (String, Int)]
//      {
//        val hadoopConf = new Configuration
//        val fileSystem = FileSystem.get(hadoopConf)
//        val inputStream = fileSystem.open(new Path(productFilePath))
//        val productsCSVReader = new BufferedReader(new InputStreamReader(inputStream))
//        var line = productsCSVReader.readLine
//        while (line != null) {
//          val splitLine = line.split(",")
//          val productId = splitLine(0)
//          val productName = splitLine(1)
//          val unitPrice = splitLine(2)
//          productsMap(productId) = (productName, unitPrice)
//          line = productsCSVReader.readLine
//        }
//        productsCSVReader.close()
//      }
//      val broadcastedMap = spark.sparkContext.broadcast(productsMap)

      val octOver50 = oct.groupBy("product")
        .agg(sum("sold").alias("sum_sold_oct"))
        .filter(row => row.getAs[Long]("sum_sold_oct") >= 50L)

      val novOver50 = nov.groupBy("product")
        .agg(sum("sold").alias("sum_sold_nov"))
        .filter(row => row.getAs[Long]("sum_sold_nov") >= 50L)

      val totalOver50 = octOver50.join(novOver50, "product")
        .withColumn("total_sold", col("sum_sold_oct") + col("sum_sold_nov"))
        .select("product", "total_sold")

      val results = broadcast(product).join(totalOver50, product.col("id") === totalOver50.col("product"))
        .withColumn("total_value", product.col("value") * totalOver50.col("total_sold"))
        .select("item", "total_sold", "total_value")

      results.show()
    } finally {
      spark.close()
    }
  }
}
