package turn

/**
  * Created by bluebyte60 on 8/5/16.
  */
class UserClickAds extends java.io.Serializable {
  var id = "0";
  var gender = "unknown";
  var adIds = Map[String, Int]();
  var clickedAdIds = Seq[String]();

  def this(userId: String, userGender: String, clickAdIds: Seq[String]) {
    this()
    id = userId;
    gender = userGender;
    adIds = clickAdIds.foldLeft(Map.empty[String, Int]) {
      (count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
    }
    clickedAdIds = clickAdIds;
  }


  override def toString(): String = {
   // gender + " " + mapToString(adIds)
    id+"|"+gender+"|"+clickedAdIds.mkString(" ");
  }


  def mapToString(m: Map[String, Int]): String = {
    var s = "";
    for ((key, value) <- m) {
      s += key + ":" + value + " "
    }
    s;
  }
}
