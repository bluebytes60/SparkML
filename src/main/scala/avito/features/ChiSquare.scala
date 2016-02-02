package avito.features

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable
import scala.reflect.io.File

/**
  * Created by bluebyte60 on 12/29/15.
  * This chi-square implementation returns a ranked list of features with corresponding chi-square value
  */
object ChiSquare {



  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val stopf = sc.wholeTextFiles("data/stopwords/").flatMap { case (filename, content) => content.split("\r\n") }.collect().toList
    val stopwords = sc.broadcast(HashSet() ++ stopf).value

    val inputDir = sc.textFile("data/reuters/train")
    val data = inputDir.map(line => {
      var tokons: List[String] = List("")
      for (term <- line.split("\\s+")) {
        val t = term.trim
        if (t.length > 2 && !stopwords.contains(t))
          tokons = tokons :+ t
      }
      tokons
    })

    val r = calculate(data, stopwords)
    //r.saveAsTextFile("ff")
  }

  def calculate(data: RDD[List[String]], stopwords: Set[String]): RDD[(String, Double)] = {

    //1. Get total frequency
    val N = data.flatMap(tokon => {
      tokon.map(x => "N")
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).values.collect()(0)

    //2. Get frequency map given a term, (key: term, value: frequency)
    val T = data.flatMap(tokons => {
      tokons.map(x => (x, 1))
    }).reduceByKey(_ + _, 1).filter { case (k, v) => v >= 5 }

    //3. Get frequency map given a category, (key:category, value: frequency)
    val C = data.map(tokons => tokons(0)).map(cat => (cat, 1)).reduceByKey(_ + _, 1).cache().collectAsMap()

    //4. Get term frequency map given a term and a category, (key:term, value: Map(category, frequency))
    val T_C = data.flatMap(tokons => {
      //count term frequency(a+c) & category frequency (a+b)
      var seq = new ListBuffer[String]();
      for (i <- 1 until tokons.length) {
        seq = seq :+ tokons(0) + File.separator + tokons(i) //cat_term, 1
      }
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).filter { case (k, v) => k.split(File.separator).length == 2 }
      .map { case (k, v) => (k.split(File.separator)(1), k.split(File.separator)(0) + File.separator + v) } //term, cat_fre
      .aggregateByKey(mutable.ParHashMap.empty[String, Int])(addToMap, mergePartitionMaps)

    //5. Start to calculate Chi-square
    val V = T.keys
    val categories = C.keys
    val combined = T.join(T_C)
    val r = combined.flatMap { case (term, pair) => {
      var seq = new ListBuffer[Tuple2[String, Double]]()
      val freqs = pair._2
      for (c <- categories) {
        var a = freqs.getOrElse(c, 0)
        val a_b = C.get(c).get
        val a_c = pair._1
        seq = seq :+ Tuple2(term, chi(a, a_b, a_c, N))
      }
      seq
    }
    }.reduceByKey(_ + _, 1).map(item => item.swap).sortByKey(true, 1).map(item => item.swap)
    r
  }


  def addToMap(s: mutable.ParHashMap[String, Int], v: String): mutable.ParHashMap[String, Int] = {
    s.put(v.split(File.separator)(0), v.split(File.separator)(1).toInt)
    s
  }

  def mergePartitionMaps(p1: mutable.ParHashMap[String, Int], p2: mutable.ParHashMap[String, Int]) = {
    p1 ++ p2
  }

  def chi(a: Int, a_b: Int, a_c: Int, N: Int): Double = {
    val b = a_b - a
    val c = a_c - a
    val b_d = N - a_c
    val d = b_d - b;
    val c_d = N - a_b;
    return (N.toFloat * Math.pow(a.toFloat * d.toFloat - b.toFloat * c.toFloat, 2)) / ((a_b.toFloat) * c_d.toFloat * a_c.toFloat * b_d.toFloat)
  }
}
