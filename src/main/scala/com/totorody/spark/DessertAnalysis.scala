package com.totorody.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkConf

object DessertAnalysis {
  def main(args : Array[String]): Unit = {
    require(args.length >= 1, "Must more than 2 arguments...")
    val dessertFilePath = args(0)
    val orderFilePath = args(1)

    val conf = new SparkConf().setAppName("DessertAnalysis")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val dessertSchema = new StructType()
      .add(StructField("menuId", StringType))
      .add(StructField("name", StringType))
      .add(StructField("price", IntegerType))
      .add(StructField("kcal", IntegerType))
    val orderSchema = new StructType()
      .add(StructField("orderId", StringType))
      .add(StructField("menuId", StringType))
      .add(StructField("num", IntegerType))

    val dessert = spark.read.format("csv")
      .schema(dessertSchema)
      .load(dessertFilePath)

    val order = spark.read.format("csv")
      .schema(orderSchema)
      .load(orderFilePath)

    dessert.cache
    order.cache

    val orderWithAmount = dessert.join(
        order, dessert.col("menuId") === order.col("menuId"), "inner")
      .select($"orderId", $"name", ($"num" * $"price") as "amount_per_menu_per_slip")
    orderWithAmount.show

    val amountPerOrder = orderWithAmount.groupBy($"orderId")
      .agg(sum($"amount_per_menu_per_slip") as "total_amount_per_slip")
      .select($"orderId", $"total_amount_per_slip")
    amountPerOrder.show

    val priceRangeDessert = dessert.select(((dessert.col("price") / 1000).cast(IntegerType) * 1000).as("price_range"), dessert("*"))
    priceRangeDessert.write
      .format("csv")
      .save("/home/totorody/data/hive/price_range_dessert_csv_non_partitioned")
    priceRangeDessert.write
      .format("csv")
      .partitionBy("price_range")
      .save("/home/totorody/data/hive/price_range_dessert_csv_partitioned")
  }
}
