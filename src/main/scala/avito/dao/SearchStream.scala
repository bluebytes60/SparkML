package avito.dao

import org.apache.spark.rdd.RDD

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 1/27/16.
  */

object SearchStream {
  def parse(data: RDD[String]): RDD[SearchStream] = {
    val r = data.filter(line => line.split("\t").length > 4).map(line => new SearchStream(line))
    r
  }
}

class SearchStream(s: String) {
  val data = s.split("\t")
  val SearchID = data(0)
  val AdID = data(1)
  val Position = data(2)
  val ObjectType = data(3)
  val HisCTR = data(4)
  val isClick = data(5)
}
