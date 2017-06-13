package com.cloudwick.kinesis

import java.nio.ByteBuffer
import java.util.List

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason
import com.amazonaws.services.kinesis.model.{PutRecordRequest, Record}
import com.cloudwick.kinesis.KinesisNewsClient._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._
import scala.util.Random


/**
 * Reads the records from the stream, fetches the confidence rating and
 * generates the ML attributes necessary for prediction
 */
class KinesisSampleIRecordProcessor extends IRecordProcessor with Serializable {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .getOrCreate()

  import spark.implicits._

  val MyAccessKeyID = getAccessKeyID()
  val MySecretKey = getSecretKey()

  val CHECKPOINT_INTERVAL_MILLIS = 30000;
  val sitesDF = spark.sparkContext
    //.textFile("/Users/Rajiv/Desktop/Capstone Project/newsSitesClean.txt")
    .textFile("s3n://"+MyAccessKeyID+":"+MySecretKey+"@fakenewsinput/newsSitesClean.txt")
    .map(_.split(","))
    .map(attributes => Sites(attributes(0), attributes(1).toFloat))
    .toDF().cache()
  var nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;

  override def initialize(shardName: String): Unit = {
    println("Started Reading Shard..." + shardName)
  }

  /* Process the records read from the input kinesis stream */
  override def processRecords(records: List[Record], checkPointer: IRecordProcessorCheckpointer): Unit = {
    println("Processing records")
    val data: Seq[Array[Byte]] = records map (_.getData.array())
    val strings = data map (i => new String(i, "UTF-8"))
    getConfidenceScores(strings)

    // Checkpoint once every checkpoint interval
    if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
      checkPointer.checkpoint()
      nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
    }
    println("Check Pointing Done")

  }

  /*  Joins the news record with site confidence score and calls a function to get ML Attributes */

  def getConfidenceScores(myRecords: Seq[String]): Unit = {


    val newsDF = myRecords.map(_.split(","))
      .map(attributes => News(attributes(0), attributes(1), attributes(2)))
      .toDF()

    val myJoinedDF = newsDF.join(sitesDF, newsDF.col("siteName") === sitesDF.col("name"), "left_outer")
    val outputDF = myJoinedDF.select($"Date", $"siteName", $"headline", $"confidenceScore")
    val outputRDD = outputDF.rdd
    val featuresRDD = outputRDD.map(row => row + "::" + getMLAttributes(row(3)))
    val parsedData = featuresRDD.map { line =>
      val partsA = line.split("::")
      val rowPart = partsA(0)
      val mlAttrib = partsA(1).split(",")
      (rowPart, LabeledPoint(mlAttrib(0).toDouble, Vectors.sparse(2, Array(0, 1), Array(mlAttrib(1).toDouble, mlAttrib(2).toDouble))))
    }
    val streamRDD = letsPredictData(parsedData)
    val partitionKey = "partitionKey"
    val myNewList = streamRDD.collect().toList
    val myKinesisClient = new AmazonKinesisClient(new BasicAWSCredentials(MyAccessKeyID, MySecretKey))
        myKinesisClient.setEndpoint("https://kinesis.us-east-1.amazonaws.com")

    // Put the processed records into a new stream for further analysis needed for Kinesis Analytics
    for (recordNum <- 0 to myNewList.size-1) {
      val partitionKey = "partitionKey"
      val putRecordRequest = new PutRecordRequest().withStreamName(kinesisStreamName)
        .withPartitionKey(partitionKey)
        .withData(ByteBuffer.wrap(myNewList(recordNum).getBytes()))
      myKinesisClient.putRecord(putRecordRequest)
    }
  }

  /* Generates vectors needed for Machine learning algorithm */
  def getMLAttributes(confScore: Any) = {

    var featureA = 0;
    var featureB: Float = 0.0f;
    val myList = Array(0, 1)
    val featureC = myList(Random.nextInt(myList.size))
    if (confScore == null) {
      featureB = 5.0f;
      featureA = 2
    }
    else {
      featureB = confScore.toString.toFloat
      if (featureB.toString.toFloat < 5.00 && featureC == 1) {
        featureA = 2
      }
      else {

        if (featureB.toString.toFloat < 5.00 && featureC == 0) {
          featureA = 0
        }
        else {
          featureA = 1
        }
      }
    }
    featureA + "," + featureB + "," + featureC
  }

  override def shutdown(checkPointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    println(" Worker Shutting down..Check pointing in progress", checkPointer.checkpoint())
  }

}