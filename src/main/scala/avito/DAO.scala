package avito

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.matching.Regex

/**
  * Created by bluebyte60 on 1/26/16.
  */
object DAO {

  class SearchStream(s: String) {
    val data = s.split("\t")
    val searchID = data(0).toLong
    val AdID = data(1).toLong
    val Position = data(2).toInt
    val ObjectType = data(3).toInt
    val HisCTR = data(4).toFloat
    val isClick = data(5).toInt
  }

  val searchTimeFormat = new java.text.SimpleDateFormat("yyyy-MM-DD HH:mm:ss.S")
  val pattern = new Regex("([0-9]+):")

  class SearchInfo(s: String) {

    var SearchID = 0L
    var SearchDate = new Date()
    var IPID = 0L
    var UserID = 0L
    var IsUserLoggedOn = false
    var SearchQuery = Set[String]()
    var LocationID = 0
    var CategoryID = 0
    var SearchParams = Set[String]()

    parseSearchInfo(s)

    def parseSearchInfo(s: String) = {
      if (s.length > 0) {
        val data = s.split("\t")
        SearchID = data(0).toLong
        SearchDate = parseSearchDate(data(1))
        IPID = data(2).toLong
        UserID = data(3).toLong
        IsUserLoggedOn = data(4).toBoolean
        SearchQuery = data(5).split(" ").toSet
        LocationID = data(6).toInt
        CategoryID = data(7).toInt
        SearchParams = parseSearchPara(data(8))
      }
    }

    def parseSearchDate(s: String): Date = {
      return searchTimeFormat.parse(s)
    }

    def parseSearchPara(s: String): Set[String] = {
      if (s == null || s.length == 0) Set()
      else pattern.findAllIn(s).map(x => x.replaceAll(":", "")).toSet
    }


  }


}
