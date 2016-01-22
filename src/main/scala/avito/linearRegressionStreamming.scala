package avito

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.apache.spark.mllib.regression.{StreamingLinearRegressionWithSGD, LabeledPoint}

import scala.collection.mutable

/**
  * Created by bluebyte60 on 1/22/16.
  */
object linearRegressionStreamming {

  def main(args: Array[String]) {
    val dbUrl = "jdbc:sqlite:/Users/bluebyte60/Documents/github/SparkML/data/avito/database.sqlite"

    val query = "SELECT isClick, searchID, adID, position, histctr, id from train_sample "

    val driver = "org.sqlite.JDBC"

    val batchSize = 100;

    val features = 4

    val sparkConf = new SparkConf().setAppName("QueueStream").setMaster("local[2]")

    val ssc = new StreamingContext(sparkConf, Milliseconds(1000))

    val rddQueue = new mutable.SynchronizedQueue[RDD[String]]()

    val trainingData = ssc.queueStream(rddQueue).map(LabeledPoint.parse).cache()

    val numFeatures = 4

    val model = new StreamingLinearRegressionWithSGD().setInitialWeights(Vectors.zeros(numFeatures))

    model.trainOn(trainingData)

    model.predictOnValues(trainingData.map(lp => (lp.label, lp.features))).print()

    ssc.start()

    new Thread(new DataSource(dbUrl, query, driver, 100, ssc, rddQueue)).start

    ssc.awaitTermination()

  }


}
