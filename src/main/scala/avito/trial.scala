package avito

import avito.dao.{SearchStream, SearchInfo}
import avito.features.ChiSquare
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 1/26/16.
  */
object trial {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis();


    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val rawSearchStream = Util.removeFirstLine(sc.textFile("/Users/bluebyte60/Desktop/avito/trainSearchStream.tsv"))
    val SearchStream = rawSearchStream.filter(line => line.split("\t").length > 4)
      .map(line => new SearchStream(line))
      .map(rawSearchStream => (rawSearchStream.SearchID, rawSearchStream.isClick))
    val rawSearchInfo = Util.removeFirstLine(sc.textFile("/Users/bluebyte60/Desktop/avito/SearchInfo.tsv"))
    val SearchInfo = rawSearchInfo.map(line => new SearchInfo(line))
      .filter(searchInfo => searchInfo.SearchParams.size > 0 || searchInfo.SearchQuery.size > 0)
      .map(searchInfo => (searchInfo.SearchID, (searchInfo.SearchQuery, searchInfo.SearchParams)))

    val combined = SearchStream.join(SearchInfo)

    val queryData: RDD[List[String]] = combined.map { case (searchID, (isClick, (query, paras))) =>
      isClick :: (query).toList
    }

    val parasData: RDD[List[String]] = combined.map { case (searchID, (isClick, (query, paras))) =>
      isClick :: (paras).toList
    }

    val queryFeatures = ChiSquare.calculate(queryData, Set(""))

    val parasFeatures = ChiSquare.calculate(parasData, Set(""))

    queryFeatures.saveAsTextFile("searchInfo/query")

    parasFeatures.saveAsTextFile("searchInfo/paras")

    val endTime = System.currentTimeMillis();

    println(startTime - endTime)

  }

}
