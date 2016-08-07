package avito

import avito.dao._
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 2/11/16.
  */
object Test {
  val shouldSave = true
  val dir = "/Users/bluebyte60/Desktop/avito/"
  val adsInfoFile = dir + "AdsInfo.tsv"
  val categoryFile = dir + "Category.tsv"
  val locationFile = dir + "Location.tsv"
  val phoneStreamFile = dir + "PhoneRequestsStream.tsv"
  val visitsStreamFile = dir + "VisitsStream.tsv"
  val searchInfoFile = dir + "SearchInfo.tsv"
  val userInfoFile = dir + "UserInfo.tsv"
  val trainSearchStreamFile = dir + "trainSearchStream.tsv"
  val chiFeature = dir + "chi"
  val tiitleFeature = dir + "title"
  val paraFeatures = dir + "paras"

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Preprocess").setMaster("local[2]")

    val sc = new SparkContext(conf)

    println(sc.textFile(adsInfoFile).partitions.size)

    println(sc.textFile(visitsStreamFile).partitions.size)

    println(sc.textFile(visitsStreamFile).partitions.size)


  }
}
