package avito

import avito.Transform.Trans
import avito.dao._
import avito.features.{PriorClicks, CatAvgPrice, ChiSquare, Feature}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 2/1/16.
  */


object Preprocess {
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


  def rmFirst(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Preprocess").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val adsInfos = AdsInfo.parse(rmFirst(sc.textFile(adsInfoFile)))

    val searchCategories = Category.parse(rmFirst(sc.textFile(categoryFile)), ActionType.Search)

    val adsCategories = Category.parse(rmFirst(sc.textFile(categoryFile)), ActionType.Ads)

    val locations = Location.parse(rmFirst(sc.textFile(locationFile)))

    val searchLocations = Location.parseSearchLocation(rmFirst(sc.textFile(locationFile)))

    val phoneStream = ContactStream.parsePhone(rmFirst(sc.textFile(phoneStreamFile)))

    val visitStream = ContactStream.parseVisit(rmFirst(sc.textFile(visitsStreamFile)))

    val searchInfos = SearchInfo.parse(rmFirst(sc.textFile(searchInfoFile)))

    val userInfo = UserInfo.parse(rmFirst(sc.textFile(userInfoFile)))

    val trainSearchStream = SearchStream.parse(rmFirst(sc.textFile(trainSearchStreamFile))).filter(s => s.ObjectType.equals("3"))

    val query = Feature.readFromfile(chiFeature, sc, 1000)

    val title = Feature.readFromfile(tiitleFeature, sc, 1000)

    val paras = Feature.readFromfile(paraFeatures, sc, -1)

    val catAvg = CatAvgPrice.parse(adsInfos).collectAsMap()

    val priorClicks = PriorClicks.parse(trainSearchStream)

    //-----------------------------------Join----------------------------------//

    val userF = Feature.appendContact(Feature.appendContact(userInfo.map(x => (x, Seq[Any]())), visitStream), phoneStream)

    val searchInfoF = Feature.appendSearchCats(Feature.appendSearchLocations(Feature.appendUserInfo(searchInfos, userF), searchLocations), searchCategories)

    val adsF = Feature.appendLocations(Feature.appendCats(adsInfos.map(x => (x, Seq[Any]())), adsCategories), locations)

    val s1 = Feature.appendAds(trainSearchStream.map(x => (x, Seq[Any](x))), adsF)

    val s2 = Feature.appendSearchInfo(s1, searchInfoF)

    val s3 = Feature.appendAds(s2, adsF)

    val s4 = Feature.appendPriorClicks(s3, priorClicks)


    //----------------------------------convertToVector------------------------//
    val r = s4.map {
      case (sea, seq) => {
        (sea.isClick,
          (Trans.baseFeature(seq) ++
            Trans.textFeatures(seq, paras, query, title) ++
            Trans.CatAvgPrice(seq, catAvg) ++
            Trans.ContactFeature(seq))
          )
      }
    }.map { case (isClick, seq) => (String.format("(%s,[%s])", isClick.toString, seq mkString ","))
    }
    r.saveAsTextFile(dir + "rr")

    //-------------------------------------------------------------------------//
  }

}
