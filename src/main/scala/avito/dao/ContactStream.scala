package avito.dao

import java.util.Date

import avito.DateUtil
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/27/16.
  */
object ContactStream {
  def parse(data: RDD[String], Type: String): RDD[(String, mutable.ParHashSet[ContactHis])] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new ContactStream(line))
      .map(vistStream => (vistStream.UserID, new ContactHis(vistStream.AdID, vistStream.ViewDate, Type)))
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

class ContactHis(AdsID: String, date: Date, Type: String) extends java.io.Serializable {
  val ContactAdsID = AdsID
  val ContactDate = date
  val ContactType = Type

  override def equals(o: Any) = o match {
    case that: ContactHis => that.ContactAdsID.equalsIgnoreCase(this.ContactAdsID) && DateUtil.hasSameDate(ContactDate, that.ContactDate) && that.ContactType.equals(ContactType)
    case _ => false
  }

  override def hashCode = ContactAdsID.hashCode + ContactDate.hashCode() + ContactType.hashCode

  override def toString() = ContactAdsID + " " + ContactDate.toString + " " + ContactType

}
