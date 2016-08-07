package turn

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{SVMWithSGD, NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.util.MLUtils

/**
  * Created by bluebyte60 on 8/6/16.
  */
object MLTraining {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("MLTraining").setMaster("local[2]")
    val sc = new SparkContext(conf)

    // Load training data in LIBSVM format
    val data = MLUtils.loadLibSVMFile(sc, "training/part-*").filter(x => x.features.numActives > 1)
    val male = data.filter(x => x.label == 0)
    val female = data.filter(x => x.label == 1)

    val sampledMale = male.randomSplit(Array(0.8, 0.2), seed = 11L)
    val sampledFemale = female.randomSplit(Array(0.8, 0.2), seed = 11L)


    val training = (sampledMale(0) ++ sampledFemale(0)).cache()
    val testing = sampledMale(1) ++ sampledFemale(1)

    //    MLUtils.saveAsLibSVMFile(training, "SVMTrain")
    //    MLUtils.saveAsLibSVMFile(testing, "SVMTest")

    // Run training algorithm to build the model
    //    val numIterations = 2000
    //  val model = SVMWithSGD.train(training, numIterations)

    val nb = new NaiveBayes().setLambda(1.0).setModelType("multinomial")
    val model = nb.run(training)

    // Compute raw scores on the test set.
    val scoreAndLabels = testing.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new MulticlassMetrics(scoreAndLabels)
    // Precision by label
    val labels = metrics.labels
    labels.foreach { l =>
      println(s"Precision($l) = " + metrics.precision(l))
    }

    // Recall by label
    labels.foreach { l =>
      println(s"Recall($l) = " + metrics.recall(l))
    }

    //    // False positive rate by label
    //    labels.foreach { l =>
    //      println(s"FPR($l) = " + metrics.falsePositiveRate(l))
    //    }

    // F-measure by label
    labels.foreach { l =>
      println(s"F1-Score($l) = " + metrics.fMeasure(l))
    }
  }
}
