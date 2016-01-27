package avito

import java.util.{Calendar, Date}

import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 1/27/16.
  */
object Util {
  val searchTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

  def removeFirstLine(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
  }

  def parseSearchDate(s: String): Date = {
    var d: Date = new Date()
    try {
      d = searchTimeFormat.parse(s)
    } catch {
      case e: Exception => {
        d = new Date()
      }
    }
    d
  }


  def hasSameDate(date1: Date, date2: Date): Boolean = {
    val cal1 = Calendar.getInstance();
    val cal2 = Calendar.getInstance();
    cal1.setTime(date1);
    cal2.setTime(date2);
    val year1 = cal1.get(Calendar.YEAR);
    val month1 = cal1.get(Calendar.MONTH);
    val day1 = cal1.get(Calendar.DAY_OF_MONTH);
    val hour1 = cal1.get(Calendar.HOUR_OF_DAY);
    val minutes1 = cal1.get(Calendar.MINUTE);
    val seconds1 = cal1.get(Calendar.SECOND);

    val year2 = cal2.get(Calendar.YEAR);
    val month2 = cal2.get(Calendar.MONTH);
    val day2 = cal2.get(Calendar.DAY_OF_MONTH);
    val hour2 = cal2.get(Calendar.HOUR_OF_DAY);
    val minutes2 = cal2.get(Calendar.MINUTE);
    val seconds2 = cal2.get(Calendar.SECOND);
    (year2 == year2 && month1 == month2 && day1 == day2 && hour1 == hour2 && minutes1 == minutes1 && seconds1 == seconds2)
  }

}
