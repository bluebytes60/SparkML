package avito.dao

import org.apache.spark.rdd.RDD

/**
  * Created by bluebyte60 on 2/1/16.
  */

object UserInfo {
  def parse(data: RDD[String]): RDD[UserInfo] = {
    val r = data.filter(x => x.split("\t").length >= 5).map(line => new UserInfo(line))
    r
  }
}

class UserInfo extends java.io.Serializable {
  var UserID = ""
  var UserAgentID = ""
  var UserAgentFamilyID = ""
  var UserAgentOSID = ""
  var UserDeviceID = ""

  def this(s: String) {
    this()
    parse(s)
  }

  def parse(s: String): Unit = {
    val data = s.split("\t")
    UserID = data(0)
    UserAgentID = data(1)
    UserAgentFamilyID = data(2)
    UserAgentOSID = data(2)
    UserDeviceID = data(3)
  }

}
