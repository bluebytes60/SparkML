package avito.features

import avito.dao.{SearchStream, SearchInfo}
import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 1/28/16.
  */
object PriorClicks {

  def parse(data: RDD[SearchStream]): RDD[(String, PriorClicks)] = {
    val r = data.map(searchStream => (searchStream.AdID, searchStream.isClick.toInt)).reduceByKey(_ + _, 1)
      .map { case (ad, clicks) => (ad, new PriorClicks(clicks)) }
    r
  }
}

class PriorClicks extends java.io.Serializable {
  var Clicks = 0;

  def this(v: Int) {
    this()
    Clicks = v
  }
}
