package avito

import avito.dao._
import avito.features.{PriorClicks, CatAvgPrice, ChiSquare, Feature}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 2/1/16.
  */

object Preprocess {
  val dir = "/Users/bluebyte60/Desktop/avito/"
  val adsInfoFile = dir + "AdsInfoSampled.tsv"
  val categoryFile = dir + "Category.tsv"
  val locationFile = dir + "Location.tsv"
  val phoneStreamFile = dir + "PhoneRequestsStreamSampled.tsv"
  val visitsStreamFile = dir + "VisitsStreamSampled.tsv"
  val searchInfoFile = dir + "SearchInfoSampled.tsv"
  val userInfoFile = dir + "UserInfo.tsv"
  val trainSearchStreamFile = dir + "VisitsStreamSampled.tsv"
  val chiFeature = dir + "chi"


  def removeFirstLine(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Preprocess").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val adsInfos = AdsInfo.parse(sc.textFile(adsInfoFile))

    val searchCategories = Category.parse(sc.textFile(categoryFile), ActionType.Search)

    val adsCategories = Category.parse(sc.textFile(categoryFile), ActionType.Ads)

    val locations = Location.parse(sc.textFile(locationFile))

    val phoneStream = ContactStream.parse(sc.textFile(phoneStreamFile), ActionType.Phone)

    val visitStream = ContactStream.parse(sc.textFile(visitsStreamFile), ActionType.Visit)

    val searchInfos = SearchInfo.parse(sc.textFile(searchInfoFile))

    val userInfo = UserInfo.parse(sc.textFile(userInfoFile))

    val trainSearchStream = SearchStream.parse(sc.textFile(trainSearchStreamFile))

    val chi = Feature.readFromfile(dir + "chi", sc, 20000)

    val paras = Feature.readFromfile(dir + "paras", sc, -1)

    val CatAvg = CatAvgPrice.parse(adsInfos).collectAsMap()

    val priorClicks = PriorClicks.parse(trainSearchStream)

    //-----------------------------------Join----------------------------------//

    val userF = Feature.appendContactStreamInfo(userInfo, phoneStream.union(visitStream).groupByKey())

    val searchInfoF = Feature.appendSearchCats(Feature.appendSearchLocations(Feature.appendUserInfo(searchInfos, userF), locations), searchCategories)

    val adsF = Feature.appendLocations(Feature.appendCats(adsInfos.map(x => (x, Seq[Any]())), adsCategories), locations)

    val TotalF = Feature.appendPriorClicks(Feature.appendSearchInfo(Feature.appendAds(trainSearchStream.map(x => (x, Seq[Any]())), adsF), searchInfoF), priorClicks)

    //----------------------------------convertToVector------------------------//


    //-------------------------------------------------------------------------//
  }


}
