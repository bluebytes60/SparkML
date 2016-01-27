import java.util.{Calendar, Date}

import avito.Util
import avito.dao.{SearchStream, SearchInfo}
import org.scalatest.FunSuite

class SearchIDAndSearchInfo extends FunSuite {

  test("Should able to parse searchStream") {
    val s = "2\t11441863\t1\t3\t0.001804\t0";
    val sea = new SearchStream(s)
    assert(sea.SearchID == "2")
    assert(sea.AdID == "11441863")
    assert(sea.Position == "1")
    assert(sea.ObjectType == "3")
    assert(sea.HisCTR == "0.001804")
    assert(sea.isClick == "0")
  }

  test("should able to parse search date") {
    val s = "2015-05-06 07:29:16.0"
    val d = Util.parseSearchDate(s)
    val cal = Calendar.getInstance();
    cal.setTime(d);
    val year = cal.get(Calendar.YEAR);
    val month = cal.get(Calendar.MONTH);
    val day = cal.get(Calendar.DAY_OF_MONTH);
    val hour = cal.get(Calendar.HOUR_OF_DAY);
    val minutes = cal.get(Calendar.MINUTE);
    val seconds = cal.get(Calendar.SECOND);
    assert(year == 2015)
    assert(month == 4) //offset by 1 (0~11)
    assert(day == 6)
    assert(hour == 7)
    assert(minutes == 29)
    assert(seconds == 16)
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
    assert(searchInfo.SearchID == "1")
    val cal = Calendar.getInstance();
    cal.setTime(searchInfo.SearchDate);
    val year = cal.get(Calendar.YEAR);
    assert(year == 2015)
    assert(searchInfo.SearchDate.getMonth == 4)
    assert(searchInfo.IPID == "1717090")
    assert(searchInfo.UserID == "3640266")
    assert(searchInfo.IsUserLoggedOn == "0")
    assert(searchInfo.SearchQuery.size == 0)
    assert(searchInfo.LocationID == "1729")
    assert(searchInfo.CategoryID == "5")
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