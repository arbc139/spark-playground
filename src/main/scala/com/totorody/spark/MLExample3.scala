package com.totorody.spark

import org.apache.spark.SparkConf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType

object MLExample3 {
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
  case class Predict(
                    describe: String,
                    avg_temp: Double,
                    rainfall: Double,
                    weekend: Double,
                    total_snowfall: Double
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
    val vectorAsblr = new VectorAssembler()
      .setInputCols { Array("avg_temp", "weekend", "rainfall") }
      .setOutputCol("input_vec")
    val scaler = new StandardScaler()
      .setInputCol(vectorAsblr.getOutputCol)
      .setOutputCol("scaled_vec")

    val linReg = new LinearRegression()
      .setMaxIter(10)
      .setFeaturesCol(scaler.getOutputCol)
      .setLabelCol("sale")
    val pipeline = new Pipeline()
      .setStages(Array(vectorAsblr, scaler, linReg))
    val regEvaluator = new RegressionEvaluator()
      .setLabelCol("sale")
    val paramGrid = new ParamGridBuilder()
      .addGrid(vectorAsblr.inputCols, Array(
        Array("avg_temp", "weekend", "rainfall"),
        Array("avg_temp", "weekend", "rainfall", "total_snowfall")
      ))
      .addGrid(linReg.maxIter, Array(5, 10, 15))
      .addGrid(scaler.withMean, Array(false))
      .build
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(regEvaluator)
      .setEstimatorParamMaps(paramGrid)
    val cvModel = crossValidator.fit(weekendSalesAndWeatherData)

    val test = spark.sparkContext
      .parallelize(Seq(
        Predict("Usually Day", 20.0, 20, 0, 0),
        Predict("Weekend", 20.0, 20.1, 1, 0),
        Predict("Cold Day", 3.0, 20, 0, 20)
      ))
      .toDF()
    cvModel.transform(test)
      .select("describe", "prediction")
      .collect
      .foreach {
        case Row(describe: String, prediction: Double) =>
          println($"($describe) -> prediction=$prediction")
      }
  }
}
