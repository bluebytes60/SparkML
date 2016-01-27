package avito.dao

/**
  * Created by bluebyte60 on 1/27/16.
  */

class SearchStream(s: String) {
  val data = s.split("\t")
  val SearchID = data(0)
  val AdID = data(1)
  val Position = data(2)
  val ObjectType = data(3)
  val HisCTR = data(4)
  val isClick = data(5)
}
