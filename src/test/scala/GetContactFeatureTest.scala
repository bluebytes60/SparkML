import java.util.Date

import avito.Transform.Trans
import avito.dao._
import org.scalatest.FunSuite

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 2/3/16.
  */
class GetContactFeatureTest extends FunSuite {
  test("Can get Contact Feature") {
    val c1 = new PhoneHis("1", new Date())
    val c2 = new PhoneHis("1", new Date())
    val c3 = new PhoneHis("2", new Date())
    var s = new SearchStream()
    s.AdID = "1"
    var sInfo = new SearchInfo()
    sInfo.SearchDate = new Date(System.currentTimeMillis() + 10000L)
    var seq = Seq(s, sInfo, mutable.ParHashSet(c1, c2))
    val r = Trans.ContactFeature(seq)

    assert(r == Seq(1, 1))
  }

}
