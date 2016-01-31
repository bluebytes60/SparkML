package avito.features

import avito.dao.{Location, Category, AdsInfo, SearchStream}
import org.apache.spark.rdd.RDD

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

  def appendLocations(adsStream: RDD[(AdsInfo, Seq[Any])], locations: RDD[Location]): RDD[(AdsInfo, Seq[Any])] = {
    val mappedAdsStream = adsStream.map { case (ad, seq) => (ad.LocationID, (ad, seq)) }
    val mappedLocation = locations.map(loc => (loc.LocationID, Seq(loc)))
    val r = mappedAdsStream.leftOuterJoin(mappedLocation)
      .map {
        case (locID, ((ad, seq), cat)) => (ad, seq ++ cat.getOrElse(Seq()))
      }
    r
  }

}
