package avito

import avito.dao.{SearchInfo, SearchStream}
import featureSelection.ChiSquare
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bluebyte60 on 1/26/16.
  */
object TextFeatureSelection2 {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis();


    val conf = new SparkConf().setAppName("chisquare")

    val sc = new SparkContext(conf)

    val rawSearchStream = Preprocess.rmFirst(sc.textFile(args(0)))
    val SearchStream = rawSearchStream.filter(line => line.split("\t").length > 4)
      .map(line => new SearchStream(line))
      .map(rawSearchStream => (rawSearchStream.AdID, rawSearchStream.isClick))
    val rawSearchInfo = Preprocess.rmFirst(sc.textFile(args(1)))
    val SearchInfo = rawSearchInfo.map(line => new SearchInfo(line))
      .filter(searchInfo => searchInfo.SearchParams.size > 0 || searchInfo.SearchQuery.size > 0)
      .map(searchInfo => (searchInfo.SearchID, (searchInfo.SearchQuery, searchInfo.SearchParams)))
    //val adsInfos = AdsInfo.parse(Preprocess.rmFirst(sc.textFile("/Users/bluebyte60/Desktop/avito/AdsInfo.tsv"))).map(ad => (ad.AdID, ad.Title.split(" ").toList.filter(x => x.length >= 3)))

    val combined = SearchStream.join(SearchInfo)

    //val titleData = combined.map { case (adID, (isClick, fea)) => isClick :: fea }

    val queryData: RDD[List[String]] = combined.map { case (searchID, (isClick, (query, paras))) =>
      isClick :: (query).toList
    }

    val queryFeatures = ChiSquare.calculate(queryData,5)
    //    val parasFeatures = ChiSquare.calculate(parasData, Set(""))
    //
    //    queryFeatures.saveAsTextFile("searchInfo/query")
    //
    //    parasFeatures.saveAsTextFile("searchInfo/paras")

    //val titleFeatures = ChiSquare.calculate(titleData, Set())
    //titleFeatures.saveAsTextFile("searchInfo/titles")
    queryFeatures.saveAsTextFile(args(2))
    val endTime = System.currentTimeMillis();

    println("time spent:"+(endTime - startTime))
    print(rawSearchStream.count())
  }

}
