package turn

import featureSelection.ChiSquare
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by bluebyte60 on 8/6/16.
  */
object FeaturePreprocess {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FeaturePreprocess").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val userClickedData = sc.textFile("data/turn/trainingData/userClickedData")
    val data = userClickedData.map(line => {
      val tokons = line.split("\\|")
      tokons(1) :: tokons(2).split(" ").toList
    })
    val r = ChiSquare.calculate(data, 1)
    r.saveAsTextFile("data/turn/trainingData/features")
  }

}
