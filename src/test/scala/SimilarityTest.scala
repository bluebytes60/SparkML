import com.rockymadden.stringmetric.similarity.DiceSorensenMetric
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/28/16.
  */
class SimilarityTest extends FunSuite {
  test("Test can calculate similarity") {
    assert(DiceSorensenMetric(1).compare("night", "nacht").get == 0.6)
    assert(DiceSorensenMetric(1).compare("context", "contact").get == 0.7142857142857143)
  }

}
