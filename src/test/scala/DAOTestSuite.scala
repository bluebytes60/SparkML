import java.util.Date

import avito.DAO.{SearchInfo, SearchStream}
import org.scalatest.FunSuite

class DAOTest extends FunSuite {

  test("Should able to parse searchStream") {
    val s = "2\t11441863\t1\t3\t0.001804\t0";
    val sea = new SearchStream(s)
    assert(sea.searchID == 2)
    assert(sea.AdID == 11441863)
    assert(sea.Position == 1)
    assert(sea.ObjectType == 3)
    assert(sea.HisCTR == 0.001804f)
    assert(sea.isClick == 0)
  }

  test("should able to parse search date") {
    val s = "2015-05-06 11:23:50.0"
    val d = new SearchInfo("").parseSearchDate(s)
    assert(d.getYear == 2015)
    assert(d.getMonth == 5)
    assert(d.getDay == 6)
    assert(d.getHours == 11)
    assert(d.getMinutes == 23)
    assert(d.getSeconds == 50)
  }

  test("should able to parse search params") {
    //case 1
    var s = "3:'Обувь', 175:'Женская одежда', 88:'36'"
    var result = new SearchInfo("").parseSearchPara(s)
    assert(result("3"))
    assert(result("175"))
    assert(result("88"))
    //case 2
    s = "{175:'Аксессуары'}"
    result = new SearchInfo("").parseSearchPara(s)
    assert(result("175"))
    assert(result("88") == false)
    //case 3
    result = new SearchInfo("").parseSearchPara(null)
    assert(result.size == 0)
  }

  test("should able to initial searchinfo correctly") {
    var s = "1\t2015-05-18 19:54:32.0\t1717090\t3640266\t0\t\t1729\t5\t"
    var searchInfo = new SearchInfo(s)
    //case 1
    assert(searchInfo.SearchID == 1)
    assert(searchInfo.SearchDate.getYear == 2015 && searchInfo.SearchDate.getMonth == 5)
    assert(searchInfo.IPID == 1717090)
    assert(searchInfo.UserID == 3640266)
    assert(searchInfo.IsUserLoggedOn == false)
    assert(searchInfo.SearchQuery.size == 0)
    assert(searchInfo.LocationID == 1729)
    assert(searchInfo.CategoryID == 5)
    assert(searchInfo.SearchParams.size == 0)
    //case 2
    s = "4\t2015-05-10 18:11:01.0\t898705\t3573776\t0\tpeg-perego go\t3960\t22\t{83:'Обувь', 175:'Женская одежда', 88:'38'}"
    searchInfo = new SearchInfo(s)
    assert(searchInfo.SearchQuery("peg-perego"))
    assert(searchInfo.SearchQuery("go"))
    assert(searchInfo.SearchParams("175"))
    assert(searchInfo.SearchParams("88"))
  }
}