import java.util.Date

import avito.Transform.Trans
import avito.dao.{SearchStream, ActionType, ContactHis}
import org.scalatest.FunSuite

import scala.collection.parallel.mutable

/**
  * Created by bluebyte60 on 2/3/16.
  */
class GetContactFeatureTest extends FunSuite {
  test("Can get Contact Feature") {
    val c1 = new ContactHis("1", new Date(), ActionType.Visit)
    val c2 = new ContactHis("1", new Date(), ActionType.Phone)
    val c3 = new ContactHis("2", new Date(), ActionType.Phone)
    var s = new SearchStream()
    s.AdID = "1"
    val seq = Seq(s, mutable.ParHashSet(c1, c2))

    val r = Trans.ContactFeature(seq)

    assert(r == Seq(1, 1))
  }

}
