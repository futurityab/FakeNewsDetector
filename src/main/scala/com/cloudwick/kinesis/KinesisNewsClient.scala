package com.cloudwick.kinesis

import com.amazonaws.auth._
import com.amazonaws.regions.Regions
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{InitialPositionInStream, KinesisClientLibConfiguration, Worker}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


case class News(Date: String, siteName: String, headLine: String)

case class Sites(name: String, confidenceScore: Float)

object KinesisNewsClient {

  val conf = new SparkConf().setAppName("Compute Confidence values").setMaster("local")
  val sc = new SparkContext(conf)
  val kinesisStreamName = "MyProcessedNewsStream"
  var MyAccessKeyID = ""
  var MySecretKey = ""
  var UnionRDD: org.apache.spark.rdd.RDD[(String, String)] = sc.emptyRDD


  def main(args: Array[String]): Unit = {

    //    // Debugging information
    //    BasicConfigurator.configure();
    //    implicit val logger = Logger(LoggerFactory.getLogger("KinesisConsumerApp"))
    //    logger.info("start reading App Config")

    if (args.length != 2) {

      println("Incorrect number of arguments, please specify AWS Access and Secret keys")
      println("Usage : KinesisSampleClient <AccessKey> <SecretKey>")
      System.exit(-1)
    }

    MyAccessKeyID = args(0)
    MySecretKey = args(1)
    val credentials: AWSCredentials = new BasicAWSCredentials(MyAccessKeyID, MySecretKey)
    val credentialsProvider = new AWSCredentialsProvider {
      def getCredentials = credentials

      def refresh: Unit = {}
    }

    // Create a client for consuming the data from the Raw Stream
    val kinesisClientLibConfiguration =
      new KinesisClientLibConfiguration("mytest", "mystream", credentialsProvider, "workerId")
        .withInitialPositionInStream(InitialPositionInStream.LATEST)
        .withRegionName(Regions.US_EAST_1.getName)

    val worker = new Worker(new KinesisSampleIRecordProcessorFactory, kinesisClientLibConfiguration);
    worker.run()

  }

  // Predict the genuineness  of the news using our Trained Model
  def letsPredictData(parsedData: RDD[(String, LabeledPoint)]): RDD[String] = {

    val trainingData = MLUtils.loadLibSVMFile(sc, "s3n://" + MyAccessKeyID + ":" + MySecretKey + "@fakenewsinput/MyTrainingData.txt")
    val numClasses = 3
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "entropy"
    val maxDepth = 5
    val maxBins = 32
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    var streamRDD: RDD[String] = sc.parallelize(Seq(""))

    val labelAndPreds = parsedData.map { point =>
      val prediction = model.predict(point._2.features)
      (point, prediction)
    }

    val modelAccuracy = labelAndPreds.filter(r => r._1._2.label == r._2).count().toDouble / parsedData.count()

    // To print Accuracy of the model
    //println("Model Accuracy  = " + modelAccuracy)

    val printRDD = labelAndPreds.map { line =>
      var flag: String = "N/A";
      if (line._2 == 0.0) {
        flag = "Fake News";
      }
      else {
        if (line._2 == 1.0) {
          flag = "Genuine News"
        }
        else {
          flag = "May be Genuine News"
        }
      }
      (line._1._1, flag)
    }

    // Batch records and before sending them to S3
    UnionRDD = UnionRDD.union(printRDD)
    UnionRDD.cache()
    // Send records only if the count is greater than 100
    if (UnionRDD.count() > 100) {
      print("Saving records")
      val jodaTime = new DateTime()
      val formatter = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ssSSS")
      // Formatting the string for appropriate analysis further
      val UnionReducedRDD = UnionRDD.reduceByKey((v1, v2) => v1, 1).map { tuple =>
        tuple._1.replaceAll("\\[", "").replaceAll("\\]", "").concat("," + tuple._2)
      }

      //newRDD.cache()
      streamRDD = UnionReducedRDD.union(streamRDD)

      // Batch the Processed records and send them to S3 for further processing using Athena
      UnionReducedRDD.saveAsTextFile("s3n://" + getAccessKeyID() + ":" + getSecretKey() + "@fakenewsprocessedinfo/Processed Record" + formatter.print(jodaTime))
      // Empty the record
      UnionRDD = sc.emptyRDD
    }
    return streamRDD
  }

  def getAccessKeyID(): String = {
    MyAccessKeyID
  }

  def getSecretKey(): String = {
    MySecretKey
  }


}