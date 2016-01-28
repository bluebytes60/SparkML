import avito.dao.AdsInfo
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/28/16.
  */


class AdsInfoTest extends FunSuite {
  test("Test can parse AdsInfo") {
    val s = "2\t992\t34\t{817:'Кузов', 5:'Запчасти', 598:'Для автомобилей'}\t750\tПередние брызговики Форд Фокус 2 родные\t0"
    val ai = new AdsInfo(s)
    assert(ai.AdID.equals("2"))
    assert(ai.LocationID.equals("992"))
    assert(ai.CategoryID.equals("34"))
    assert(ai.Params.size == 3)
    assert(ai.Price == 750)
    assert(ai.Title.equals("Передние брызговики Форд Фокус 2 родные"))
    assert(ai.IsContext.equals("0"))
  }
}
