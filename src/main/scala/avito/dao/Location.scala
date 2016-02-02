package avito.dao

import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 1/29/16.
  */

object Location {

  def parse(data: RDD[String]): RDD[Location] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new Location(line))
    r
  }

  def parseSearchLocation(data: RDD[String]): RDD[SearchLocation] = {
    val r = data.filter(line => line.split("\t").length >= 4).map(line => new SearchLocation(line))
    r
  }
}


class Location extends java.io.Serializable {
  var LocationID = ""
  var Level = ""
  var RegionID = ""
  var CityID = ""

  def this(s: String) {
    this()
    parse(s)
  }

  def parse(s: String): Unit = {
    val features = s.split("\t")
    LocationID = features(0)
    Level = features(1)
    RegionID = features(2)
    CityID = features(3)
  }

}

class SearchLocation extends java.io.Serializable {
  var LocationID = ""
  var Level = ""
  var RegionID = ""
  var CityID = ""

  def this(s: String) {
    this()
    parse(s)
  }

  def parse(s: String): Unit = {
    val features = s.split("\t")
    LocationID = features(0)
    Level = features(1)
    RegionID = features(2)
    CityID = features(3)
  }

}
