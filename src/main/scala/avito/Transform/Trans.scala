package avito.Transform

import java.util.Calendar

import avito.dao._
import avito.features.{CatAvgPrice, PriorClicks, Feature}
import com.rockymadden.stringmetric.similarity.DiceSorensenMetric

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 2/2/16.
  */
object Trans {

  def baseFeature(seq: Seq[Any]): Seq[Any] = {
    var v = Seq[Any]();

    val searchStream = Feature.extract(seq, classOf[SearchStream]).asInstanceOf[Option[SearchStream]]
    if (searchStream != None) {
      val s = searchStream.get
      v ++= Seq(s.SearchID, s.AdID, s.Position, s.HisCTR)
    } else throw new Exception("should have searchStream")


    val searchInfo = Feature.extract(seq, classOf[SearchInfo]).asInstanceOf[Option[SearchInfo]]
    if (searchInfo != None) {
      val s = searchInfo.get
      val cal = Calendar.getInstance();
      cal.setTime(s.SearchDate);
      v ++= Seq(
        cal.get(Calendar.DAY_OF_MONTH),
        cal.get(Calendar.DAY_OF_WEEK),
        s.IPID,
        s.UserID,
        s.IsUserLoggedOn,
        s.SearchQuery.size,
        s.LocationID,
        s.CategoryID
      )
    } else v ++= Seq(0, 0, 0, 0, 0, 0, 0, 0)

    val userInfo = Feature.extract(seq, classOf[UserInfo]).asInstanceOf[Option[UserInfo]]
    if (userInfo != None) {
      val u = userInfo.get
      v ++= Seq(u.UserAgentID, u.UserAgentFamilyID, u.UserAgentOSID, u.UserDeviceID)
    } else v ++= Seq(0, 0, 0, 0)


    val adsInfo = Feature.extract(seq, classOf[AdsInfo]).asInstanceOf[Option[AdsInfo]]
    if (adsInfo != None) {
      val a = adsInfo.get
      v ++= Seq(a.AdID, a.CategoryID, a.Title.length, a.Price, a.IsContext)
    } else v ++= Seq(0, 0, 0, 0, 0)


    val priorClick = Feature.extract(seq, classOf[PriorClicks]).asInstanceOf[Option[PriorClicks]]
    if (priorClick != None) {
      val p = priorClick.get
      v ++= Seq(p.Clicks)
    } else v ++= Seq(0)

    val searchCategory = Feature.extract(seq, classOf[Category], ActionType.Search).asInstanceOf[Option[Category]]

    if (searchCategory != None) {
      val s = searchCategory.get
      v ++= Seq(s.CategoryID, s.Level, s.ParentCategoryID, s.SubcategoryID)
    } else v ++= Seq(0, 0, 0, 0)

    val adCategory = Feature.extract(seq, classOf[Category], ActionType.Ads).asInstanceOf[Option[Category]]

    if (adCategory != None) {
      val a = adCategory.get
      v ++= Seq(a.CategoryID, a.Level, a.ParentCategoryID, a.SubcategoryID)
    } else v ++= Seq(0, 0, 0, 0)

    val searchLocation = Feature.extract(seq, classOf[SearchLocation]).asInstanceOf[Option[SearchLocation]]

    if (searchLocation != None) {
      val s = searchLocation.get
      v ++= Seq(s.LocationID, s.Level, s.RegionID, s.CityID)
    } else v ++= Seq(0, 0, 0, 0)

    val adsLocation = Feature.extract(seq, classOf[Location]).asInstanceOf[Option[Location]]

    if (adsLocation != None) {
      val a = adsLocation.get
      v ++= Seq(a.LocationID, a.Level, a.RegionID, a.CityID)
    } else v ++= Seq(0, 0, 0, 0)

    v
  }

  def textFeatures(seq: Seq[Any], paramFeatures: scala.collection.Map[String, Double], queryFeatures: scala.collection.Map[String, Double], titleFeatures: scala.collection.Map[String, Double]): Seq[Any] = {
    var v = Seq[Any]()
    val searchInfo = Feature.extract(seq, classOf[SearchInfo]).asInstanceOf[Option[SearchInfo]]

    val adsInfo = Feature.extract(seq, classOf[AdsInfo]).asInstanceOf[Option[AdsInfo]]

    if (searchInfo != None && searchInfo.get.SearchParams.size > 0
      && adsInfo != None && adsInfo.get.Params.size > 0) {
      v ++= Seq(searchInfo.get.SearchParams.intersect(adsInfo.get.Params).size)
    } else v ++= Seq(0)

    if (searchInfo != None) {
      val s = searchInfo.get
      val paraSim = s.SearchParams.filter(x => paramFeatures.contains(x)).map(x => paramFeatures(x))
      if (!paraSim.isEmpty) {
        v ++= Seq(paraSim.sum / paraSim.size, paraSim.min, paraSim.max)
      } else v ++= Seq(0, 0, 0)
    } else v ++= Seq(0, 0, 0)

    if (searchInfo != None) {
      val s = searchInfo.get
      val querySim = s.SearchQuery.filter(x => queryFeatures.contains(x)).map(x => queryFeatures(x))
      if (!querySim.isEmpty) {
        v ++= Seq(querySim.sum / querySim.size, querySim.min, querySim.max)
      } else v ++= Seq(0, 0, 0)
    } else v ++= Seq(0, 0, 0)

    if (adsInfo != None) {
      val a = adsInfo.get
      val titleSim = a.Title.split(" ").filter(x => titleFeatures.contains(x)).map(x => titleFeatures(x))
      if (!titleSim.isEmpty) {
        v ++= Seq(titleSim.sum / titleSim.size, titleSim.min, titleSim.max)
      } else v ++= Seq(0, 0, 0)
    } else v ++= Seq(0, 0, 0)

    if (searchInfo != None && adsInfo != None) {
      val value = searchInfo.get.SearchQuery.map(x => DiceSorensenMetric(1).compare(x, adsInfo.get.Title).getOrElse(0d)).sum
      v ++= Seq(value)
    } else v ++= Seq(0)
    v
  }

  def CatAvgPrice(seq: Seq[Any], catAvg: scala.collection.Map[String, CatAvgPrice]): Seq[Any] = {
    var v = Seq[Any]()

    val adsInfo = Feature.extract(seq, classOf[AdsInfo]).asInstanceOf[Option[AdsInfo]]
    var v1, v2 = 0d
    if (adsInfo != None) {
      val a = adsInfo.get
      v1 = if (catAvg.contains(a.CategoryID)) catAvg(a.CategoryID).Avg else 0
      v ++= Seq(v1)
    } else v ++= Seq(0)

    val searchInfo = Feature.extract(seq, classOf[SearchInfo]).asInstanceOf[Option[SearchInfo]]
    if (searchInfo != None) {
      val s = searchInfo.get
      v2 = if (catAvg.contains(s.CategoryID)) catAvg(s.CategoryID).Avg else 0
      v ++= Seq(v2)
    } else v ++= Seq(0)
    v ++= Seq(if (v1 > v2) 1 else 0)
    v
  }

  def ContactFeature(seq: Seq[Any]): Seq[Any] = {
    ContactFeature(seq, ActionType.Phone) ++ ContactFeature(seq, ActionType.Visit)
  }

  def ContactFeature(seq: Seq[Any], Type: String): Seq[Any] = {
    var v = Seq[Any]()
    val searchStream = Feature.extract(seq, classOf[SearchStream]).asInstanceOf[Option[SearchStream]]
    val contactFeature = Feature.extract(seq, classOf[mutable.ParHashSet[ContactHis]]).asInstanceOf[Option[mutable.ParHashSet[ContactHis]]]
    val searchInfo = Feature.extract(seq, classOf[SearchInfo]).asInstanceOf[Option[SearchInfo]]
    if (searchStream != None && contactFeature != None && searchInfo != None) {
      val s = searchStream.get
      val c = contactFeature.get
      val se = searchInfo.get
      v ++= Seq(c.filter(
        x => x.ContactType.equals(Type)
          && x.ContactAdsID.equals(s.AdID)
          && x.ContactDate.before(se.SearchDate)).size)
    } else v ++= Seq(0)
    v
  }


}
