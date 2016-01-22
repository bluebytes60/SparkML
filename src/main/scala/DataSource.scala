import java.util

import org.apache.commons.dbcp2._

import scala.collection.mutable.ListBuffer

class DataSource(dbUrl: String, query: String, driver: String, batchSize: Int) {

  var hasFinish = false
  var lastID = 0l;
  val connectionPool = new BasicDataSource()
  connectionPool.setDriverClassName(driver)
  connectionPool.setUrl(dbUrl)
  connectionPool.setInitialSize(3)

  def isTerminated(): Boolean = hasFinish

  def getData(): List[util.ArrayList[String]] = {
    var r = new ListBuffer[util.ArrayList[String]]()
    val connection = connectionPool.getConnection
    val stmt = connection.createStatement()
    val rs = stmt.executeQuery(query + " where id > " + lastID + " order by id asc limit " + batchSize)
    var total = 0;
    while (rs.next()) {
      val c = rs.getMetaData.getColumnCount
      var data = new util.ArrayList[String]()
      for (idx <- 1 to c) {
        if (rs.getMetaData.getColumnName(idx).equals("id"))
          lastID = rs.getLong(idx)
        else
          data.add(rs.getString(idx))
      }
      r += data
      total += 1;
    }
    if (total == 0) hasFinish = true
    connection.close();
    stmt.close()
    r.toList
  }
}

//object Test {
//  def main(args: Array[String]) {
//    val dbUrl = "jdbc:sqlite:/Users/bluebyte60/Documents/github/SparkML/data/avito/database.sqlite"
//    val query = "SELECT * from train_sample"
//    val driver = "org.sqlite.JDBC"
//    val batchSize = 100;
//    val ds = new DataSource(dbUrl, query, driver, 100)
//    var count = 0;
//    while (!ds.isTerminated()) {
//      for (row <- ds.getData()) {
//        println(row)
//        count += 1
//      }
//    }
//    println("count = " + count)
//  }
//}