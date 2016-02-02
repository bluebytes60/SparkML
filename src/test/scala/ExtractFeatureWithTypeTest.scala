import avito.dao.{SearchInfo, ActionType, Category, Location}
import avito.features.Feature
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 2/3/16.
  */
class ExtractFeatureWithTypeTest extends FunSuite {
  test("Test can extract feature given type") {
    var cat1 = new Category()
    cat1.CategoryType = ActionType.Search
    cat1.CategoryID = "99"
    var cat2 = new Category()
    cat2.CategoryType = ActionType.Ads
    cat2.CategoryID = "100"
    val seq = Seq(cat1, cat2)

    val v1 = Feature.extract(seq, classOf[Category], ActionType.Search).asInstanceOf[Option[Category]].get
    assert(v1.CategoryID.equals("99"))

    val v2 = Feature.extract(seq, classOf[Category], ActionType.Ads).asInstanceOf[Option[Category]].get
    assert(v2.CategoryID.equals("100"))

  }
}
