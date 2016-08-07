package avito

import java.io.{File, IOException}

import avito.Transform.Trans
import avito.dao._
import avito.features.{PriorClicks, CatAvgPrice, Feature}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scalax.file.Path

/**
  * Created by bluebyte60 on 2/1/16.
  */


object Preprocess {

  def rmFirst(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  }

  def main(args: Array[String]) {

    val dir = args(0)
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

    val conf = new SparkConf().setAppName("Preprocess").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val hadoopConf = sc.hadoopConfiguration

    hadoopConf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")

    hadoopConf.set("fs.s3.awsAccessKeyId", args(1))

    hadoopConf.set("fs.s3.awsSecretAccessKey", args(2))

    val adsInfos = AdsInfo.parse(rmFirst(sc.textFile(adsInfoFile)))

    val searchCategories = Category.parse(rmFirst(sc.textFile(categoryFile)), ActionType.Search)

    val adsCategories = Category.parse(rmFirst(sc.textFile(categoryFile)), ActionType.Ads)

    val locations = Location.parse(rmFirst(sc.textFile(locationFile)))

    val searchLocations = Location.parseSearchLocation(rmFirst(sc.textFile(locationFile)))

    val phoneStream = ContactStream.parsePhone(rmFirst(sc.textFile(phoneStreamFile,5000)))

    val visitStream = ContactStream.parseVisit(rmFirst(sc.textFile(visitsStreamFile,5000)))

    val searchInfos = SearchInfo.parse(rmFirst(sc.textFile(searchInfoFile,5000)))

    val userInfo = UserInfo.parse(rmFirst(sc.textFile(userInfoFile)))

    val trainSearchStream = SearchStream.parse(rmFirst(sc.textFile(trainSearchStreamFile,5000))).filter(s => s.ObjectType.equals("3"))

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

    r.saveAsTextFile(args(3))

    //-------------------------------------------------------------------------//
  }

}
