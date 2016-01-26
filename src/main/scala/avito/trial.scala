package avito

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 1/26/16.
  */
object trial {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")
    val sc = new SparkContext(conf)
    ///Users/bluebyte60/Desktop/avito/trainSearchStream.tsv
    val searchStream = sc.textFile("/Users/bluebyte60/Desktop/avito/trainSearchStream.tsv")
    searchStream.foreach(s => println(s))

  }

}
