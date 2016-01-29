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

class SearchStream extends java.io.Serializable{
  var SearchID = ""
  var AdID = ""
  var Position = ""
  var ObjectType = ""
  var HisCTR = ""
  var isClick = ""

  def this(s: String) {
    this()
    parse(s)
  }

  def parse(s: String) {
    val data = s.split("\t")
    SearchID = data(0)
    AdID = data(1)
    Position = data(2)
    ObjectType = data(3)
    HisCTR = data(4)
    isClick = data(5)
  }
}
