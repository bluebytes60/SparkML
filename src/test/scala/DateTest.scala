import avito.DateUtil
import avito.dao.ContactHis
import org.scalatest.FunSuite

import scala.collection.immutable.HashSet
;

/**
  * Created by bluebyte60 on 1/27/16.
  */
class DateTest extends FunSuite {
  test("Test can checkout if two dates are equal or not") {
    val s1 = "2015-05-06 07:29:16.0"
    val d1 = DateUtil.parseSearchDate(s1)
    val d2 = DateUtil.parseSearchDate(s1)
    assert(DateUtil.hasSameDate(d1, d2))
  }

  test("Test hashcode working correctly") {
    val s = HashSet()
    val d1 = "2015-05-06 07:29:16.0"
    val VisHist1 = new ContactHis("1", DateUtil.parseSearchDate(d1),"phone")
    val VisHist2 = new ContactHis("1", DateUtil.parseSearchDate(d1),"phone")
    val set1 = HashSet(VisHist1, VisHist2)
    assert(set1.size == 1)
    val d2 = "2015-05-08 07:29:16.0"
    val VisHist3 = new ContactHis("1", DateUtil.parseSearchDate(d2),"phone")
    val set2 = HashSet(VisHist1, VisHist2, VisHist3)
    assert(set2.size == 2)
  }
}
