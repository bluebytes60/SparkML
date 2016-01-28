package avito.dao

import java.util.Date

import avito.Util
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/27/16.
  */
object ContactStream {
  def parse(data: RDD[String]): RDD[(String, mutable.ParHashSet[ContactHis])] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new ContactStream(line))
      .map(vistStream => (vistStream.UserID, new ContactHis(vistStream.AdID, vistStream.ViewDate)))
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

class ContactStream(s: String) {
  val features = s.split("\t")
  val UserID = features(0)
  val IPID = features(1)
  val AdID = features(2)
  val ViewDate = Util.parseSearchDate(features(3))
}

class ContactHis(AdsID: String, date: Date) extends java.io.Serializable {
  val ContactAdsID = AdsID
  val ContactDate = date

  override def equals(o: Any) = o match {
    case that: ContactHis => that.ContactAdsID.equalsIgnoreCase(this.ContactAdsID) && Util.hasSameDate(ContactDate, that.ContactDate)
    case _ => false
  }

  override def hashCode = ContactAdsID.hashCode + ContactDate.hashCode()

  override def toString() = ContactAdsID + " " + ContactDate.toString

}
