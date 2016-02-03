package avito.features

import avito.dao._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/30/16.
  */
object Feature {

  def extract(seq: Seq[Any], c: Class[_]): Option[Any] = {
    var Op: Option[Any] = None
    for (x <- seq) {
      if (x.getClass.equals(c)) Op = Some(x)
    }
    Op
  }

  def extract(seq: Seq[Any], c: Class[_], Type: String): Option[Any] = {
    var Op: Option[Any] = None
    for (x <- seq) {
      if (x.getClass.equals(c) && c.equals(classOf[Category]) && x.asInstanceOf[Category].CategoryType.equals(Type)) Op = Some(x)
    }
    Op
  }

  def appendSearchInfo(searchStream: RDD[(SearchStream, Seq[Any])], searchInfos: RDD[(SearchInfo, Seq[Any])]): RDD[(SearchStream, Seq[Any])] = {
    val mappedSearchStream = searchStream.map { case (s, seq) => (s.AdID, (s, seq)) }
    val mappedSeachInfos = searchInfos.map { case (se, seq) => (se.SearchID, seq :+ se) }
    val r = mappedSearchStream.leftOuterJoin(mappedSeachInfos)
      .map {
        case (adsID: String, ((s, seq1), seq2)) => (s, seq1 ++ seq2.getOrElse(Seq()))
      }
    r
  }

  def appendPriorClicks(searchStream: RDD[(SearchStream, Seq[Any])], priorClicks: RDD[(String, PriorClicks)]): RDD[(SearchStream, Seq[Any])] = {
    val mappedSearchStream = searchStream.map { case (s, seq) => (s.AdID, (s, seq)) }
    val priorclicks = priorClicks.map { case (se, prior) => (se, Seq(se)) }
    val r = mappedSearchStream.leftOuterJoin(priorclicks)
      .map {
        case (adsID: String, ((s, seq1), seq2)) => (s, seq1 ++ seq2.getOrElse(Seq()))
      }
    r
  }


  def appendAds(searchStream: RDD[(SearchStream, Seq[Any])], ads: RDD[(AdsInfo, Seq[Any])]): RDD[(SearchStream, Seq[Any])] = {
    val mappedSearchStream = searchStream.map { case (s, seq) => (s.AdID, (s, seq)) }
    val mappedAds = ads.map { case (ad, seq) => (ad.AdID, seq :+ ad) }
    val r = mappedSearchStream.leftOuterJoin(mappedAds)
      .map {
        case (adsID: String, ((s, seq1), seq2)) => (s, seq1 ++ seq2.getOrElse(Seq()))
      }
    r
  }

  def appendCats(adsStream: RDD[(AdsInfo, Seq[Any])], cats: RDD[Category]): RDD[(AdsInfo, Seq[Any])] = {
    val mappedAdsStream = adsStream.map { case (ad, seq) => (ad.CategoryID, (ad, seq)) }
    val mappedCategory = cats.map(cat => (cat.CategoryID, Seq(cat)))
    val r = mappedAdsStream.leftOuterJoin(mappedCategory)
      .map {
        case (catID, ((ad, seq), cat)) => (ad, seq ++ cat.getOrElse(Seq()))
      }
    r
  }

  def appendSearchCats(adsStream: RDD[(SearchInfo, Seq[Any])], cats: RDD[Category]): RDD[(SearchInfo, Seq[Any])] = {
    val mappedAdsStream = adsStream.map { case (ad, seq) => (ad.CategoryID, (ad, seq)) }
    val mappedCategory = cats.map(cat => (cat.CategoryID, Seq(cat)))
    val r = mappedAdsStream.leftOuterJoin(mappedCategory)
      .map {
        case (catID, ((ad, seq), cat)) => (ad, seq ++ cat.getOrElse(Seq()))
      }
    r
  }

  def appendLocations(adsStream: RDD[(AdsInfo, Seq[Any])], locations: RDD[Location]): RDD[(AdsInfo, Seq[Any])] = {
    val mappedAdsStream = adsStream.map { case (ad, seq) => (ad.LocationID, (ad, seq)) }
    val mappedLocation = locations.map(loc => (loc.LocationID, Seq(loc)))
    val r = mappedAdsStream.leftOuterJoin(mappedLocation)
      .map {
        case (locID, ((ad, seq), cat)) => (ad, seq ++ cat.getOrElse(Seq()))
      }
    r
  }

  def appendSearchLocations(adsStream: RDD[(SearchInfo, Seq[Any])], locations: RDD[Location]): RDD[(SearchInfo, Seq[Any])] = {
    val mappedAdsStream = adsStream.map { case (ad, seq) => (ad.LocationID, (ad, seq)) }
    val mappedLocation = locations.map(loc => (loc.LocationID, Seq(loc)))
    val r = mappedAdsStream.leftOuterJoin(mappedLocation)
      .map {
        case (locID, ((ad, seq), cat)) => (ad, seq ++ cat.getOrElse(Seq()))
      }
    r
  }

  def appendUserInfo(searchStream: RDD[SearchInfo], userInfos: RDD[(UserInfo, Seq[Any])]): RDD[(SearchInfo, Seq[Any])] = {
    val mappedSearchInfoStream = searchStream.map { case (se) => (se.UserID, (se, Seq[Any]())) }
    val mappedUserInfos = userInfos.map { case (userInfo, seq) => (userInfo.UserID, seq :+ userInfo) }
    val r = mappedSearchInfoStream.leftOuterJoin(mappedUserInfos)
      .map {
        case (usrID, ((se, seq), userInfos)) => (se, seq ++ userInfos.getOrElse(Seq()))
      }
    r
  }

  def appendContactStreamInfo(userInfos: RDD[UserInfo], contact: RDD[(String, Iterable[mutable.ParHashSet[ContactHis]])]): RDD[(UserInfo, Seq[Any])] = {
    val mappedUserInfos = userInfos.map(user => (user.UserID, (user, Seq[Any]())))
    val mappedcontact = contact.map { case (userID, contactStream) => (userID, contactStream.toSeq) }
    val r = mappedUserInfos.leftOuterJoin(mappedcontact).map {
      case (userID, ((ad, seq), contactStreams)) => (ad, seq ++ contactStreams.getOrElse(Seq()))
    }
    r
  }

  def readFromfile(filePath: String, sc: SparkContext, topK: Int): scala.collection.Map[String, Double] = {
    val inputFile = sc.textFile(filePath)
    val r = inputFile.map(line => line.replace("(", "").replace(")", "").split(","))
      .filter(seq => seq.length == 2)
      .map(features => (features(0), features(1).toDouble))
      .map(item => item.swap).sortByKey(false, 1).map(item => item.swap)
    if (topK == -1) r.collectAsMap() else r.take(topK).toMap
  }

}
