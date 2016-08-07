package turn

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.immutable.ListMap

/**
  * Created by bluebyte60 on 8/6/16.
  */
object MLPreprocess {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MLPreprocess").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rawData = sc.textFile("data/turn/trainingData/userClickedData");
    val idxMap = Util.toIndexMap(Util.readFeaturesFromfile("data/turn/trainingData/features/part-*", sc, 3000));
    val result = rawData.map(line => toVector(line, idxMap)).saveAsTextFile("training")
  }

  def toVector(line: String, idxMap: scala.collection.Map[String, Integer]): String = {
    val tokons = line.split("\\|")
    var vector: Map[Long, Int] = Map()
    val label = tokons(1)
    for (x <- tokons(2).split(" ")) {
      if (idxMap.contains(x)) {
        val idx = idxMap.get(x).get;
        val c = vector.getOrElse[Int](x.toLong, 0)
        vector = vector + (idx.toLong -> (c + 1))
      }
    }
    vector = ListMap(vector.toSeq.sortBy(_._1): _*)
    var r = ""
    r += tokons(1).toInt - 1
    for ((k, v) <- vector) {
      r += " " + k + ":" + v
    }
    r
  }
}
