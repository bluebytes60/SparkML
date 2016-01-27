package avito.dao

import java.util

import org.apache.commons.dbcp2._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataSource(dbUrl: String, query: String, driver: String, batchSize: Int,
                 ssc: StreamingContext,
                 queue: mutable.SynchronizedQueue[RDD[String]]) extends Runnable {

  var hasFinish = false
  var lastID = 0l;
  val connectionPool = new BasicDataSource()
  connectionPool.setDriverClassName(driver)
  connectionPool.setUrl(dbUrl)
  connectionPool.setInitialSize(3)

  def isTerminated(): Boolean = hasFinish

  def getData(): List[String] = {
    var r = new ListBuffer[String]()
    val connection = connectionPool.getConnection
    val stmt = connection.createStatement()
    val rs = stmt.executeQuery(query + " where objectType =3 and id > " + lastID + " order by id asc limit " + batchSize)
    var total = 0;
    while (rs.next()) {
      val c = rs.getMetaData.getColumnCount
      var data = new util.ArrayList[String]()
      val label = rs.getString(1);
      var features = new Array[String](c - 2) //ignore id and label
      for (idx <- 2 to c - 1) {
        if (rs.getMetaData.getColumnName(idx).equals("id"))
          lastID = rs.getLong(idx)
        else
          features(idx - 2) = rs.getString(idx)
      }
      r += parse(label, features)
      total += 1;
    }
    if (total == 0) hasFinish = true
    connection.close();
    stmt.close()
    r.toList
  }

  def parse(label: String, features: Array[String]): String = {
    String.format("(%s, [%s])", label, features mkString ",")
  }

  def run {
    while (!isTerminated()) {
      val data = ssc.sparkContext.parallelize(getData())
      queue += data
    }
  }


}

//
//object Test {
//  def main(args: Array[String]) {
//    val dbUrl = "jdbc:sqlite:/Users/bluebyte60/Documents/github/SparkML/data/avito/database.sqlite"
//    val query = "SELECT isClick, searchID, adID, position, histctr, id from train_sample "
//    val driver = "org.sqlite.JDBC"
//    val batchSize = 100;
//    val ds = new avito.dao.DataSource(dbUrl, query, driver, 100000, null, null)
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