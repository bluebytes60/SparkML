package turn

import org.apache.spark.sql.{SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 5/27/16.
  */
object AUPPreprocess {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AUPPreprocess")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val parquetFile = sqlContext.read.parquet(args(0))
    parquetFile.registerTempTable("aup")

    val impression = sqlContext.sql("select userId, clicks,actions, tapActions, demographics from aup ")
    impression.map(toUser)
      .filter(user => !user.gender.equals("unknown"))
      .filter(user => user.adIds.size > 0)
      .map(user => user.toString())
      .saveAsTextFile(args(1))
  }

  def toUser(row: org.apache.spark.sql.Row) = {
    val userID = if (row.getAs("userId") == null) "unknown" else row.getAs("userId").toString
    val (clickedUserGender, clickedAdIds) = getAdsandGender(row, 1)
    val (actionUserGender, actionAdIds) = getAdsandGender(row, 2)
    val (tapActionUserGender, tapActionAdIds) = getAdsandGender(row, 3)
    val demoGraphicsGender = getDemographicGender(row)
    var defaultGender = "unknown"
    defaultGender = replaceGender(defaultGender, clickedUserGender)
    defaultGender = replaceGender(defaultGender, actionUserGender)
    defaultGender = replaceGender(defaultGender, tapActionUserGender)
    if (demoGraphicsGender != "unknown") {
      defaultGender = demoGraphicsGender
    }
    new UserClickAds(userID, defaultGender, clickedAdIds ++ actionAdIds ++ tapActionAdIds)
  }

  def replaceGender(a: String, b: String): String = {
    if (a == "unknown" && b != "unknown") b else a
  }

  def getAdsandGender(row: org.apache.spark.sql.Row, idx: Int): (String, Seq[String]) = {
    var clickedAdIds = Seq[String]();
    var userGender = "unknown"
    var impressions = row.getAs[Seq[org.apache.spark.sql.Row]](idx)
    if (impressions == null) impressions = Seq()
    impressions.foreach {
      var oldTimestamp = 0;
      impression => {
        val data = impression.getAs[org.apache.spark.sql.Row]("impression")
        val newTimeStamp = data.getAs[Int]("timeStampInSeconds")
        if (newTimeStamp > oldTimestamp && (data.getAs("gender") != null) || data.getAs("extGender") != null) {
          if (data.getAs("gender") != null) {
            userGender = data.getAs("gender").toString
          } else if (data.getAs("extGender") != null) {
            userGender = data.getAs("extGender").toString
          }
          if (data.getAs("adId") != null) {
            clickedAdIds = data.getAs("adId").toString +: clickedAdIds
          }
          oldTimestamp = newTimeStamp
        }
      }
    }
    (userGender, clickedAdIds)
  }

  def getDemographicGender(row: org.apache.spark.sql.Row): String = {
    var userGender = "unknown"
    var demographics = row.getAs[Seq[org.apache.spark.sql.Row]]("demographics")
    if (demographics == null) demographics = Seq()
    demographics.foreach {
      var oldTimestamp = 0;
      demographic => {
        val newTimeStamp = demographic.getAs[Int]("lastModifiedTimeInSeconds")
        if (newTimeStamp > oldTimestamp && (demographic.getAs("genderId") != null)) {
          if (demographic.getAs("genderId") != null) {
            userGender = demographic.getAs("genderId").toString
          }
          oldTimestamp = newTimeStamp
        }
      }
    }
    userGender
  }
}


