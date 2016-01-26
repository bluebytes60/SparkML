package avito

/**
  * Created by bluebyte60 on 1/26/16.
  */
object DAO {

  class SearchStream(s: String){
    val data = s.split("\t")
    val searchID = data(0).toLong
    val AdID = data(1).toLong
    val Position = data(2).toInt
    val ObjectType = data(3).toInt
    val HisCTR = data(4).toFloat
    val isClick = data(5).toInt


  }

  def main(args: Array[String]) {
    val s = "2\t11441863\t1\t3\t0.001804\t0";
    val sea = new SearchStream(s)
    println(sea.AdID)
  }

}
