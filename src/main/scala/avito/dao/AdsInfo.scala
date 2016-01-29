package avito.dao

import avito.Util
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable
import scala.util.matching.Regex

/**
  * Created by bluebyte60 on 1/28/16.
  */

object AdsInfo {
  def parse(data: RDD[String]): RDD[AdsInfo] = {
    val r = data.filter(line => line.split("\t").length >= 7).map(line => new AdsInfo(line))
    r
  }
}

class AdsInfo extends java.io.Serializable{

  val pattern = new Regex("([0-9]+):")

  var AdID = ""
  var LocationID = ""
  var CategoryID = ""
  var Params = Set[String]()
  var Price = 0
  var Title = ""
  var IsContext = ""

  def this(s: String) {
    this()
    parse(s)
  }

  def parse(s: String): Unit = {
    val data = s.split("\t")
    AdID = data(0)
    LocationID = data(1)
    CategoryID = data(2)
    Params = parsePara(data(3))
    Price = data(4).toInt
    Title = data(5)
    IsContext = data(6)
  }


  def parsePara(s: String): Set[String] = {
    if (s == null || s.length == 0) Set()
    else pattern.findAllIn(s).map(x => x.replaceAll(":", "")).toSet
  }
}
