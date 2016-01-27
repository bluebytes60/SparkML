package avito.dao

import java.util.Date

import avito.Util

/**
  * Created by bluebyte60 on 1/27/16.
  */
class VisitStream {

}

class VistHis(AdsID: String, date: Date) {
  val Vist = AdsID
  val VistDate = date

  override def equals(o: Any) = o match {
    case that: VistHis => that.Vist.equalsIgnoreCase(this.Vist) && Util.hasSameDate(VistDate, that.VistDate)
    case _ => false
  }

  override def hashCode = Vist.hashCode + VistDate.hashCode()

}
