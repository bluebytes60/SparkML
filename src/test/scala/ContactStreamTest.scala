import avito.dao.ContactStream
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

class ContactStreamTest extends FunSuite {
  test("Test can covert viststream data") {
    val s1 = "61291\t1769215\t10847086\t2015-04-25 00:00:01.0"
    val s2 = "61291\t1769215\t9210111\t2015-04-25 00:00:23.0"
    val s3 = "61291\t1769215\t14425445\t2015-04-25 00:01:33.0"
    val s4 = "501897\t158476\t16129123\t2015-04-25 00:02:27.0"

    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val data = sc.parallelize(List(s1, s2, s3, s4))
    val vistStream = ContactStream.from(data)
    val result = vistStream.collectAsMap()
    assert(result.keys.size == 2)
    assert(result("61291").size == 3)
    assert(result("501897").size == 1)
  }
}
