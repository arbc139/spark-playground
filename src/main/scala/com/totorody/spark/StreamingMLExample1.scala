package com.totorody.spark

import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Encoders, Row, SparkSession}
import org.apache.spark.sql.types.{StructField, StructType}

object StreamingMLExample1 {
  object Behavior extends Enumeration {
    type Behavior = Value
    val WALKING = Value(1)
    val WALKING_UPSTAIRS = Value(2)
    val WALKING_DOWNSTAIRS = Value(3)
    val SITTING = Value(4)
    val STANDING = Value(5)
    val LAYING = Value(6)

    implicit class EnumsMethod(enums: Value) {
      def getLabel: String = enums match {
        case WALKING => "WALKING"
        case WALKING_UPSTAIRS => "WALKING_UPSTAIRS"
        case WALKING_DOWNSTAIRS => "WALKING_DOWNSTAIRS"
        case SITTING => "SITTING"
        case STANDING => "STANDING"
        case LAYING => "LAYING"
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val xTrainFilePath = args(0)
    val yTrainFilePath = args(1)

    val conf = new SparkConf().setAppName("StreamingMLExample1")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    val rawXTrain = spark.read.format("csv")
      .option("header", "false")
      .load(xTrainFilePath)
    val rawYTrain = spark.read.format("csv")
      .option("header", "false")
      .load(yTrainFilePath)

    val xTrainRDD = rawXTrain.rdd
      .map(row => row.mkString.trim.split("[\\s]+"))
      .map(row => row.map(col => col.toDouble))
      .map(row => Vectors.dense(row))
    val yTrainRDD = rawYTrain.rdd
      .map(row => row.mkString.trim.toLong)

    println(xTrainRDD.count)
    println(yTrainRDD.count)

    val trainData = yTrainRDD.zip(xTrainRDD)
      .map(row => LabeledPoint(
        row._1,
        row._2
      ))
      .toDF()
    trainData.show
  }
}
