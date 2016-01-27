package avito.dao

import java.util.Date

import avito.Util

import scala.util.matching.Regex

/**
  * Created by bluebyte60 on 1/27/16.
  */

class SearchInfo(s: String) {
  val pattern = new Regex("([0-9]+):")
  var SearchID = ""
  var SearchDate = new Date()
  var IPID = ""
  var UserID = ""
  var IsUserLoggedOn = ""
  var SearchQuery = Set[String]()
  var LocationID = ""
  var CategoryID = ""
  var SearchParams = Set[String]()

  parseSearchInfo(s)

  def parseSearchInfo(s: String) = {
    if (s.length > 0) {
      val data = s.split("\t")
      if (data.length >= 1) SearchID = data(0)
      if (data.length >= 2) SearchDate = Util.parseSearchDate(data(1))
      if (data.length >= 3) IPID = data(2)
      if (data.length >= 4) UserID = data(3)
      if (data.length >= 5) IsUserLoggedOn = data(4)
      if (data.length >= 6 && data(5).length > 0) SearchQuery = data(5).split(" ").filter((x => x.length > 1)).toSet
      if (data.length >= 7) LocationID = data(6)
      if (data.length >= 8) CategoryID = data(7)
      if (data.length >= 9) SearchParams = parseSearchPara(data(8))
    }
  }


  def parseSearchPara(s: String): Set[String] = {
    if (s == null || s.length == 0) Set()
    else pattern.findAllIn(s).map(x => x.replaceAll(":", "")).toSet
  }


}
