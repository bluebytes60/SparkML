import avito.dao.{Location, AdsInfo, SearchInfo}
import avito.features.Feature
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/30/16.
  */
class ExtractFeatureTest extends FunSuite {

  test("Test can extract feature") {
    val s1 = new SearchInfo()
    s1.CategoryID = "cats"
    val s2 = new AdsInfo()
    s2.AdID = "ads"
    val seq = Seq(s1, s2)
    val searchInfo = Feature.extract(seq, classOf[SearchInfo]).asInstanceOf[Option[SearchInfo]]
    assert(searchInfo.get.CategoryID.equals("cats"))

    val adsInfo = Feature.extract(seq, classOf[AdsInfo]).asInstanceOf[Option[AdsInfo]]
    assert(adsInfo.get.AdID.equals("ads"))

    val nothing = Feature.extract(seq, classOf[Location]).asInstanceOf[Option[Location]]
    assert(nothing == None)

    for (x <- seq) {
      x match {
        case a: AdsInfo => assert(a.AdID.equals("ads"))
        case s: SearchInfo => assert(s.CategoryID.equals("cats"))
      }
    }
  }

}
