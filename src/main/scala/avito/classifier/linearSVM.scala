package avito.classifier

import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by bluebyte60 on 1/22/16.
  */
object linearSVM {

  def main(args: Array[String]) {
    val dbUrl = "jdbc:sqlite:/Users/bluebyte60/Documents/github/SparkML/data/avito/database.sqlite"

    val table = "train"

    val driver = "org.sqlite.JDBC"

    val features = 4

    val conf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val jdbcDF = sqlContext.read.format("jdbc").options(
      Map("url" -> dbUrl, "dbtable" -> table)).load()

    val data = jdbcDF.map(row => LabeledPoint.parse(toString(row)))

    val splits = data.randomSplit(Array(0.97, 0.03), seed = 11L)

    val training = splits(0).cache()

    val test = splits(1)

    // Run training algorithm to build the model
    val numIterations = 2000

    val model = SVMWithSGD.train(training, numIterations)

    // Clear the default threshold.
    model.clearThreshold()

    // Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    // Get evaluation metrics.
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)

    // Precision by threshold
    //    val precision = metrics.precisionByThreshold
    //    precision.foreach { case (t, p) =>
    //      println(s"Threshold: $t, Precision: $p")
    //    }

    // Recall by threshold
    //    val recall = metrics.recallByThreshold
    //    recall.foreach { case (t, r) =>
    //      println(s"Threshold: $t, Recall: $r")
    //    }

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val auROC = metrics.areaUnderROC()
    println("Area under ROC = " + auROC)

    //    // Save and load model
    //    model.save(sc, "myModelPath")
    //    val sameModel = SVMModel.load(sc, "myModelPath")

  }

  def toString(row: Row): String = {
    var features = new Array[Any](row.length - 1)
    for (idx <- 1 to row.length - 1) features(idx - 1) = String.valueOf(row(idx))
    String.format("(%s, [%s])", row.getLong(0).toString,
      features mkString ",")
  }

}
