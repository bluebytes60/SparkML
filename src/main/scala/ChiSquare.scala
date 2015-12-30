import java.io.{PrintWriter, File}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ListBuffer

/**
 * Created by bluebyte60 on 12/29/15.
 */
object ChiSquare {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val inputDir = sc.wholeTextFiles("data/parsed/")
    val lines = inputDir.flatMap { case (filename, content) => content.split("\n") }.cache()
    //1. Get total frequency map
    val Total = lines.flatMap(line => {
      val tokons = line.split(" ")
      var seq = new ListBuffer[String]();
      for (a <- 1 until tokons.length) {
        seq = seq :+ "N"
      }
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).cache()
    //2. Get frequency map given a term
    val T = lines.flatMap(line => {
      val tokons = line.split(" ")
      var seq: List[String] = tokons.map(_.trim).toList
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).cache().collectAsMap()
    //3. Get frequency given a category
    val C = lines.flatMap(line => {
      val tokons = line.split(" ")
      //count  category frequency (a+b)
      var seq: List[String] = List(tokons(0))
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).cache().collectAsMap()
    //4. Get term frequency map given a term and a category
    val T_C = lines.flatMap(line => {
      val tokons = line.split(" ")
      //count term frequency(a+c) & category frequency (a+b)
      var seq = new ListBuffer[String]();
      for (a <- 1 until tokons.length) {
        seq = seq :+ tokons(0) + "_" + tokons(a)
      }
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).cache()
    //5. Start to calculate Chi-square
    val N = Total.values.collect()(0)
    val V = T.keys
    val categories = C.keys
    val r = T_C.flatMap { case (key: String, value: Int) => {
      var seq = new ListBuffer[String]();
      val cat = key.split("_")(0)
      val term = key.split("_")(1)
      val a = value
      val a_b = C.get(cat).get
      val a_c = T.get(term).get
      seq = seq :+ (cat + "_" + term + ":" + chi(a, a_b, a_c, N))
      seq
    }
    }.map(word => (word.split(":")(0), word.split(":")(1).toDouble)).reduceByKey(_ + _, 1).
      map(item => item.swap).sortByKey(true, 1).map(item => item.swap).saveAsTextFile("Chisquare_result")
  }

  def chi(a: Int, a_b: Int, a_c: Int, N: Int): Double = {
    val b = a_b - a
    val c = a_c - a
    val b_d = N - a_c
    val d = b_d - b;
    val c_d = N - a_b;
    return (N * Math.pow(a.toFloat * d.toFloat - b.toFloat * c.toFloat, 2)) / ((a_b.toFloat) * c_d.toFloat * a_c.toFloat * b_d.toFloat)
  }
}
