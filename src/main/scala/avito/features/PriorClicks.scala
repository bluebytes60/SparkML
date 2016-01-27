package avito.features

import avito.dao.{SearchStream, SearchInfo}
import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 1/28/16.
  */
object PriorClicks {

  def parse(data: RDD[SearchStream]): RDD[(String, Int)] = {
    val r = data.map(searchStream => (searchStream.AdID, searchStream.isClick.toInt)).reduceByKey(_ + _, 1)
    r
  }
}
