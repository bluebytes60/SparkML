/**
 * Created by bluebyte60 on 12/28/15.
 */

import org.apache.spark.{SparkContext, SparkConf}

object WordCount  {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val inputfile = sc.textFile("hdfs://localhost:9000/user/bluebyte60/input.txt")
    val wordCounts = inputfile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _, 1).map(item => item.swap).sortByKey(true, 1).map(item => item.swap)
    wordCounts.saveAsTextFile("output")
  }
}
