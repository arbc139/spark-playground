package com.totorody.spark

import com.twitter.penguin.korean.TwitterKoreanProcessor
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

object MLExample1 {
  def relationWords(w1: String, w2: String, target: String, model: Word2VecModel): Array[(String, Double)] = {
    val b = breeze.linalg.Vector(model.getVectors(w1))
    val a = breeze.linalg.Vector(model.getVectors(w2))
    val c = breeze.linalg.Vector(model.getVectors(target))

    val x = c + (a - b)

    model.findSynonyms(Vectors.dense(x.toArray.map(_.toDouble)), 10)
  }

  def main(args: Array[String]): Unit = {
    val textPath = args(0)

    val conf = new SparkConf()
      .registerKryoClasses(Array(classOf[KryoSerializer]))
    val sc = new SparkContext(conf)
    val input = sc.textFile(textPath).map(TwitterKoreanProcessor.normalize)
      .map(TwitterKoreanProcessor.tokenize)
      .map(TwitterKoreanProcessor.tokensToStrings)

    val word2vec = new Word2Vec()
    word2vec.setMinCount(3)
    word2vec.setVectorSize(30)
    val model = word2vec.fit(input)

    relationWords("남자", "여자", "왕", model).foreach(println)
  }
}
