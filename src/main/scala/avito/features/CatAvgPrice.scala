package avito.features

import avito.dao.AdsInfo
import org.apache.spark.rdd.RDD


/**
  * Created by bluebyte60 on 1/28/16.
  */
object CatAvgPrice {

  def get(data: RDD[AdsInfo]): RDD[(String, Double)] = {
    val r = data.map(adsInfo => (adsInfo.CategoryID, (adsInfo.Price, 1)))
      .reduceByKey { case ((value1, count1), (value2, count2)) => (value1 + value2, count1 + count2) }
      .mapValues { case (value, count) => value.toDouble / count.toDouble }
    r
  }
}
