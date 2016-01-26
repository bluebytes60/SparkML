import avito.DAO.SearchStream
import org.scalatest.FunSuite

class DAOTest extends FunSuite {

  test("Should able to parse searchStream correctly") {
    val s = "2\t11441863\t1\t3\t0.001804\t0";
    val sea = new SearchStream(s)
    assert(sea.searchID==2)
    assert(sea.AdID==11441863)
    assert(sea.Position==1)
    assert(sea.ObjectType==3)
    assert(sea.HisCTR==0.001804f)
    assert(sea.isClick==0)
  }
}