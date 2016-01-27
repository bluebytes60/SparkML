package avito.dao

import java.util.Date

import avito.Util
import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/27/16.
  */
object VisitStream {
  def from(data: RDD[String]): RDD[(String, mutable.ParHashSet[VistHis])] = {
    val r = data.map(line => {
      val features = line.split("\t")
      new VisitStream(features(0), features(1), features(2), Util.parseSearchDate(features(3)))
    }).map(vistStream => (vistStream.UserID, new VistHis(vistStream.AdID, vistStream.ViewDate)))
      .aggregateByKey(mutable.ParHashSet.empty[VistHis])(addToSet, mergePartitionSet)
    r
  }

  def addToSet(s: mutable.ParHashSet[VistHis], v: VistHis): mutable.ParHashSet[VistHis] = {
    s += (v)
    s
  }

  def mergePartitionSet(p1: mutable.ParHashSet[VistHis], p2: mutable.ParHashSet[VistHis]) = {
    p1 ++ p2
  }
}

class VisitStream(userID: String, iPID: String, adID: String, viewDate: Date) {
  val UserID = userID
  val IPID = iPID
  val AdID = adID
  val ViewDate = viewDate
}

class VistHis(AdsID: String, date: Date) extends java.io.Serializable {
  val VistAdsID = AdsID
  val VistDate = date

  override def equals(o: Any) = o match {
    case that: VistHis => that.VistAdsID.equalsIgnoreCase(this.VistAdsID) && Util.hasSameDate(VistDate, that.VistDate)
    case _ => false
  }

  override def hashCode = VistAdsID.hashCode + VistDate.hashCode()

  override def toString() = VistAdsID + " " + VistDate.toString

}
