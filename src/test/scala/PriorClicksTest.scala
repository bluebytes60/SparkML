import avito.dao.SearchStream
import avito.features.PriorClicks
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/28/16.
  */
class PriorClicksTest extends FunSuite {

  test("Test can calculate prior clicks from search stream") {
    var sc: SparkContext = null
    try {
      val s1 = "2\t11441863\t1\t3\t0.001804\t1"
      val s3 = "2\t11441864\t1\t3\t0.001804\t0"
      val s2 = "2\t11441863\t1\t3\t0.001804\t1"

      val conf = new SparkConf().setAppName("priorClick").setMaster("local[2]")
      sc = new SparkContext(conf)

      val data = sc.parallelize(List(s1, s2, s3)).map(line => new SearchStream(line))

      val r = PriorClicks.parse(data)

      val result = r.collectAsMap()

      assert(result.size == 2)
      assert(result("11441863") == 2)
      assert(result("11441864") == 0)
    }
    finally {
      sc.stop()
    }

  }
}
