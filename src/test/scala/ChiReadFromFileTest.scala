import java.io.{PrintWriter}

import avito.features.{Feature, ChiSquare}
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/29/16.
  */
class ChiReadFromFileTest extends FunSuite {
  test("Test can collect ChiSquare features from file") {
    val f = java.io.File.createTempFile("pre", "post")
    val w = new PrintWriter(f)
    w.write("(samsung,669471.9934518016)\n" +
      "(кровать,670499.6433613537)\n" +
      "(кроссовки,685431.0485501128)\n" +
      "(стол,824136.2683974179)\n" +
      "(велосипеды,893819.4645119015)\n" +
      "(холодильник,1126742.830769163)\n" +
      "(платье,1143115.12640711)\n" +
      "(для,1174451.3688681324)\n" +
      "(коляска,1557036.9166462992)\n" +
      "(iphone,2626155.957430137)\n" +
      "(велосипед,3363503.55382717)")
    w.close()
    val conf = new SparkConf().setAppName("chiTest").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val chi = Feature.readFromfile(f.getAbsolutePath, sc, 10)
    assert(chi.size == 10)
    assert(chi("велосипед") == 3363503.55382717)
    assert(chi("iphone") == 2626155.957430137)
    assert(chi.getOrElse("samsung", null) == null)
    f.delete()
    sc.stop()
  }
}
