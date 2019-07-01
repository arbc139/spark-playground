package com.totorody.spark

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{LabeledPoint, StandardScaler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

object MLExample2 {
  case class Weather(
                    date: String,
                    day_of_week: String,
                    avg_temp: Double,
                    max_temp: Double,
                    min_temp: Double,
                    rainfall: Double,
                    daylight_hours: Double,
                    max_depth_snowfall: Double,
                    total_snowfall: Double,
                    solar_radiation: Double,
                    mean_wind_speed: Double,
                    max_wind_speed: Double,
                    max_instantaneous_wind_speed: Double,
                    avg_humidity: Double,
                    avg_cloud_cover: Double
                    )
  case class Sales(
                   date: String,
                   sale: Double
                   )

  def main(args: Array[String]): Unit = {
    val weatherFilePath = args(0)
    val salesFilePath = args(1)

    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[KryoSerializer]))
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val weatherSchema = ScalaReflection.schemaFor[Weather]
      .dataType
      .asInstanceOf[StructType]
    val weatherData = spark.read.format("csv")
      .schema(weatherSchema)
      .option("header", "false")
      .option("dateFormat", "yyyy-MM-dd")
      .load(weatherFilePath)
    val salesSchema = ScalaReflection.schemaFor[Sales]
      .dataType
      .asInstanceOf[StructType]
    val salesData = spark.read.format("csv")
      .schema(salesSchema)
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd")
      .load(salesFilePath)

    val isWeekend = udf((col: String) => {
      col match {
        case col if col.contains("일") || col.contains("토") => 1d
        case _ => 0d
      }
    })

    val salesAndWeatherData = weatherData.join(salesData, "date")
    val weekendSalesAndWeatherData = salesAndWeatherData.withColumn("weekend", isWeekend(salesAndWeatherData.col("day_of_week")))
      .drop("day_of_week")
      .select("sale", "avg_temp", "rainfall", "weekend")

    val labelPoints = weekendSalesAndWeatherData.map {
      row => LabeledPoint(row.getDouble(0), Vectors.dense(
        row.getDouble(1),
        row.getDouble(2),
        row.getDouble(3)
      ))
    }
    val scalerModel = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)
      .fit(labelPoints)
    val scaledData = scalerModel.transform(labelPoints)
        .drop("features")
        .select(col("label"), col("scaledFeatures").alias("features"))
    scaledData.show
    scaledData.cache


    val Array(trainData, testData) = scaledData.randomSplit(Array(0.6, 0.4), seed = 11L)

    val linRegModel = new LinearRegression()
        .setMaxIter(20)
        .fit(trainData)
    val scoreAndLabels = testData.map {
      row => (
        linRegModel.predict(row.getAs[org.apache.spark.ml.linalg.Vector]("features")),
        row.getAs[Double]("label")
      )
    }
    val metrics = new RegressionMetrics(scoreAndLabels.rdd)
    println(metrics.rootMeanSquaredError)

    val targetVector1 = Seq((null, Vectors.dense(15.0, 15.4, 1)))
      .toDF("label", "features")
    val targetVector2 = Seq((null, Vectors.dense(20.0, 0, 0)))
      .toDF("label", "features")
    val scaledTargetVector1: org.apache.spark.ml.linalg.Vector = scalerModel.transform(targetVector1)
      .select(col("scaledFeatures").alias("features"))
      .first
      .getAs[org.apache.spark.ml.linalg.Vector]("features")
    val scaledTargetVector2: org.apache.spark.ml.linalg.Vector = scalerModel.transform(targetVector2)
      .select(col("scaledFeatures").alias("features"))
      .first
      .getAs[org.apache.spark.ml.linalg.Vector]("features")
    val predict1 = linRegModel.predict(scaledTargetVector1)
    val predict2 = linRegModel.predict(scaledTargetVector2)

    println(predict1)
    println(predict2)
    linRegModel.coefficients.toArray.foreach(println)
  }
}
