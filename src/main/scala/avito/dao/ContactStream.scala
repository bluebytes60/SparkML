package avito.dao

import java.util.Date

import avito.DateUtil
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/27/16.
  */
object ContactStream {
  def parseVisit(data: RDD[String]): RDD[(String, mutable.ParHashSet[ContactHis])] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new ContactStream(line))
      .map(vistStream => (vistStream.UserID, new PhoneHis(vistStream.AdID, vistStream.ViewDate)))
      .aggregateByKey(mutable.ParHashSet.empty[ContactHis])(addToSet, mergePartitionSet)
    r
  }

  def parsePhone(data: RDD[String]): RDD[(String, mutable.ParHashSet[ContactHis])] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new ContactStream(line))
      .map(vistStream => (vistStream.UserID, new PhoneHis(vistStream.AdID, vistStream.ViewDate)))
      .aggregateByKey(mutable.ParHashSet.empty[ContactHis])(addToSet, mergePartitionSet)
    r
  }

  def addToSet(s: mutable.ParHashSet[ContactHis], v: ContactHis): mutable.ParHashSet[ContactHis] = {
    s += (v)
    s
  }

  def mergePartitionSet(p1: mutable.ParHashSet[ContactHis], p2: mutable.ParHashSet[ContactHis]) = {
    p1 ++ p2
  }
}

class ContactStream(s: String) extends java.io.Serializable {
  val features = s.split("\t")
  val UserID = features(0)
  val IPID = features(1)
  val AdID = features(2)
  val ViewDate = DateUtil.parseSearchDate(features(3))
}

class ContactHis(AdsID: String, date: Date) extends java.io.Serializable {
  val ContactAdsID = AdsID
  val ContactDate = date

  override def equals(o: Any) = o match {
    case that: ContactHis => that.ContactAdsID.equalsIgnoreCase(this.ContactAdsID) && DateUtil.hasSameDate(ContactDate, that.ContactDate)
    case _ => false
  }

  override def hashCode = ContactAdsID.hashCode + ContactDate.hashCode()

  override def toString() = ContactAdsID + " " + ContactDate.toString + " "

}

class PhoneHist(AdsID: String, date: Date) extends ContactHis(AdsID: String, date: Date)

class PhoneHis(AdsID: String, date: Date) extends ContactHis(AdsID: String, date: Date)