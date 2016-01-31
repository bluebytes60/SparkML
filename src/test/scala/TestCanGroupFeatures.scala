import avito.dao.{Category, AdsInfo, SearchStream}
import avito.features.Feature
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/29/16.
  */
class TestCanGroupFeatures extends FunSuite {
  test("Test can group features") {
    var s1 = new SearchStream()
    s1.SearchID = "1"
    s1.AdID = "111"

    var ad1 = new AdsInfo()
    ad1.AdID = "111"
    ad1.CategoryID = "c"

    var ad2 = new AdsInfo()
    ad2.AdID = "222"
    ad2.CategoryID = "d"

    var cat = new Category()
    cat.CategoryID = "c"
    cat.Level = "1"

    val conf = new SparkConf().setAppName("TestCanGroupFeatures").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val searchStreams = sc.parallelize(Seq(s1)).map(searchStream => (searchStream, Seq[Any]()))
    val ads = sc.parallelize(Seq(ad1, ad2)).map(ad => (ad, Seq[Any]()))
    val cats = sc.parallelize(Seq(cat))

    val r1 = Feature.appendAds(searchStreams, Feature.appendCats(ads, cats)).collect()(0)

    assert(r1._1.SearchID.equals("1"))
    assert(r1._1.AdID.equals("111"))

    val v1 = r1._2

    v1.foreach {
      case ad: AdsInfo => assert(ad.AdID.equals("111") && ad.CategoryID.equals("c"))
      case cat: Category => assert(cat.CategoryID.equals("c") && cat.Level.equals("1"))
    }

    sc.stop()
  }

}
