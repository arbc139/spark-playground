package com.totorody.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object GraphXExample1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder.config(conf).getOrCreate()
    val users: RDD[(VertexId, (String, String))] = spark.sparkContext
      .parallelize(Array(
        (3L, ("taro", "woker")),
        (7L, ("jiro", "worker")),
        (5L, ("saburo", "chief")),
        (2L, ("shiro", "manager"))
      ))
    val relationships: RDD[Edge[String]] = spark.sparkContext
      .parallelize(Array(
        Edge(3L, 7L, "friend"),
        Edge(5L, 3L, "advisor"),
        Edge(2L, 5L, "colleague"),
        Edge(5L, 7L, "friend")
      ))
    val defaultUser = ("John Doe", "Missing")
    val graph = Graph(users, relationships, defaultUser)

    val chiefVerticeCount = graph.vertices
      .filter {
        case (id, (name, pos)) => pos == "chief"
      }
      .count
    println(chiefVerticeCount)
    graph.inDegrees
      .collect
      .foreach(println)
  }
}
