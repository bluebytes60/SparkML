import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.tools.ant.taskdefs.Available.FileDir

/**
 * Created by bluebyte60 on 12/29/15.
 */
object ChiSquareRound1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("chisquare").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val inputDir = sc.wholeTextFiles("data/parsed/")
    val lines = inputDir.flatMap { case (filename, content) => content.split("\n") }
    val r = lines.flatMap(line => {
      val tokons = line.split(" ")
      //count term frequency(a+c) & category frequency (a+b)
      var seq: List[String] = tokons.map(_.trim).toList
      //count total frequency a+b+c+D
      seq = seq :+ "N";
      //count term to category frequency (a)
      for (a <- 1 until tokons.length) {
        seq = seq :+ tokons(0) + "_" + tokons(a)
      }
      seq
    }).map(word => (word, 1)).reduceByKey(_ + _, 1).sortByKey(true, 1)
    r.saveAsTextFile("test_output")
  }
}
