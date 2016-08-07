package turn

import org.apache.spark.SparkContext

/**
  * Created by bluebyte60 on 8/6/16.
  */
object Util {

  def readFeaturesFromfile(filePath: String, sc: SparkContext, topK: Int): scala.collection.Map[String, Double] = {
    val inputFile = sc.textFile(filePath)
    val r = inputFile.map(line => line.replace("(", "").replace(")", "").split(","))
      .filter(seq => seq.length == 2)
      .map(features => (features(0), features(1).toDouble))
      .map(item => item.swap).sortByKey(false, 1).map(item => item.swap)
    if (topK == -1) r.collectAsMap() else r.take(topK).toMap
  }

  def toIndexMap(inputMap: scala.collection.Map[String, Double]): scala.collection.Map[String, Integer] = {
    var c: Int = 1;
    var idxMap: Map[String, Integer] = Map();
    for ((k, _) <- inputMap) {
      idxMap = idxMap + (k -> c)
      c += 1
    }
    idxMap
  }
}
