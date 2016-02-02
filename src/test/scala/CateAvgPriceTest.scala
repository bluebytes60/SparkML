import avito.dao.AdsInfo
import avito.features.CatAvgPrice
import org.apache.spark.{SparkContext, SparkConf}
import org.scalatest.FunSuite

/**
  * Created by bluebyte60 on 1/29/16.
  */
class CateAvgPriceTest extends FunSuite {
  test("Test can get mean price for category ID") {
    val s1 = "2\t992\t31\t{817:'Кузов', 5:'Запчасти', 598:'Для автомобилей'}\t750\tПередние брызговики Форд Фокус 2 родные\t0"
    val s2 = "2\t992\t31\t{817:'Кузов', 5:'Запчасти', 598:'Для автомобилей'}\t500\tПередние брызговики Форд Фокус 2 родные\t0"
    val s3 = "2\t992\t32\t{817:'Кузов', 5:'Запчасти', 598:'Для автомобилей'}\t750\tПередние брызговики Форд Фокус 2 родные\t0"
    val conf = new SparkConf().setAppName("priorClick").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(List(s1, s2, s3))
    val adsInfos = AdsInfo.parse(data)
    val stats = CatAvgPrice.parse(adsInfos).collectAsMap()
    assert(stats("31").Avg == 625)
    assert(stats("32").Avg == 750)
    sc.stop()
  }
}
