package com.totorody.spark

import org.apache.spark.SparkConf
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object GraphXExample2 {
  /*
  station_cd, station_g_cd, station_name, station_name_k,
  station_name_r, line_cd, pref_cd, add,
  lon, lat, open_ymd, close_ymd,
  e_status, e_sort

  1110101, 1110101, HAKODATE, ${EMPTY},
  ${EMPTY}, 11101, 1, hokkaido hakodate-shi wakamatsucho,
  140.726413, 41.773709, 12/10/1902, ${EMPTY},
  0, 1110101
   */
  case class Station(
                    station_cd: Integer,
                    station_g_cd: Integer,
                    station_name: String,
                    station_name_k: String,
                    station_name_r: String,
                    line_cd: Integer,
                    pref_cd: Integer,
                    add: String,
                    lon: Double,
                    lat: Double,
                    open_ymd: String,
                    close_ymd: String,
                    e_status: Integer,
                    e_sort: Integer
                    )
  /*
    line_cd, station_cd1, station_cd2
    1002, 100201, 100202
   */
  case class Join(
                 line_cd: Integer,
                 station_cd1: Integer,
                 station_cd2: Integer
                 )

  def main(args: Array[String]): Unit = {
    val stationFilePath = args(0)
    val joinFilePath = args(1)

    val startStationCode = args(2)
    val goalStationCode = args(3)

    val conf = new SparkConf().setAppName("GraphXExample2")
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._

    val stationSchema = ScalaReflection.schemaFor[Station]
      .dataType
      .asInstanceOf[StructType]
    val stations = spark.read.format("csv")
      .schema(stationSchema)
      .option("header", "true")
      .option("dateFormat", "mm/dd/yyyy")
      .load(stationFilePath)
    val joinSchema = ScalaReflection.schemaFor[Join]
      .dataType
      .asInstanceOf[StructType]
    val joins = spark.read.format("csv")
      .schema(joinSchema)
      .option("header", "true")
      .load(joinFilePath)
    val stationsRDD: RDD[(VertexId, (String, Long, String))] = stations
      .map {
        row => {
          val vertexId = row.getAs[Integer]("station_cd")
          val elements = (
            row.getAs[String]("station_name"),
            0L,
            ""
          )
          (vertexId.toLong, elements)
        }
      }
      .rdd
    val joinStationsRDD: RDD[Edge[(Long, Int)]] = joins
      .flatMap {
        row => Seq(
          Edge(
            row.getAs[Integer]("station_cd1").toLong,
            row.getAs[Integer]("station_cd2").toLong,
            (1L, row.getAs[Integer]("line_cd").toInt)
          ),
          Edge(
            row.getAs[Integer]("station_cd2").toLong,
            row.getAs[Integer]("station_cd1").toLong,
            (1L, row.getAs[Integer]("line_cd").toInt)
          )
        )
      }
      .rdd
    val sameStationsRDD: RDD[Edge[(Long, Int)]] = stations
      .flatMap {
        row => {
          val code = row.getAs[Integer]("station_cd")
          val group_code = row.getAs[Integer]("station_g_cd")
          if (code == group_code) Seq.empty
          else Seq(
            Edge(code.toInt, group_code.toInt, (0L, -1)),
            Edge(group_code.toInt, code.toInt, (0L, -1))
          )
        }
      }
      .rdd

    val graph = Graph(
      stationsRDD, joinStationsRDD.union(sameStationsRDD), ("", 0L, "")
    )
    val subgraph = graph.subgraph(
      edge => (edge.attr._2 >= 28001 && edge.attr._2 <= 28010) || (edge.attr._2 == -1),
      (a, b) => true
    )
    val sssp = subgraph.pregel[(Long, String)](
      (10000000000L, ""),
      20,
      EdgeDirection.Either
    )(
      (stationCode, state, message) => {
        if (stationCode == startStationCode) (state._1, 0, state._1)
        else (state._1, message._1, message._2)
      },
      triplet => {
        if(triplet.dstAttr._3 == "" || triplet.srcAttr._2 + triplet.attr._1 <= triplet.dstAttr._2) {
          if(triplet.attr._1 == 0) Iterator((triplet.dstId, ((triplet.srcAttr._2 + triplet.attr._1), triplet.srcAttr._3)))
          else Iterator((triplet.dstId, ((triplet.srcAttr._2 + triplet.attr._1), triplet.srcAttr._3 + "->" + triplet.dstAttr._1)))
        } else {
          Iterator.empty
        }
      },
      (message1, message2) => if(message1._1 < message2._1) message1 else message2
    )

    println(sssp.vertices
      .filter(i => i._1 == goalStationCode)
      .collect
      .mkString("\n"))

    spark.stop()
  }
}
