package org.apache.spark.edu.wpi.dsrg.adisax

import org.apache.commons.lang.SerializationUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.log4j.Logger
import org.apache.log4j.Level


import org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.utils.Util

/**
  * Created by leon on 7/6/17.
  */
object RandWalkTsCreater extends Logging {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("TEST").setMaster("local[*]")
    //  val conf = new SparkConf().setAppName("TEST").setMaster("local[*]")
    val sc = new SparkContext(conf)




    val result: RDD[(Text, Text)] = sc.sequenceFile("hdfs://localhost:9000/user/ajaji/input/random-walk-1", classOf[Text], classOf[Text])




    val rdd = result.collect()

    println(rdd.getClass())
    //println(result.count())

    //generateTsAndSave(sc)

  }
  def generateTsAndSave(sc: SparkContext): Unit = {

    val tsRdd = generateTsRdd(sc,
      1000,
      10000,
      320,
      1234)

    try {
      val savePath = "hdfs://localhost:9000/user/ajaji/input/random-walk-1"
      tsRdd.saveAsSequenceFile(savePath)
      writeLog(("==> Random Walk generate Time series successfully! %s").format(savePath))

    } catch {
      case e: Exception => logError(e.toString)
        System.exit(0)
    }
  }

  def generateTsRdd(sc: SparkContext, nbr: Long, length: Int, partitionNbr: Int, seed: Long): RDD[(Long, String)] = {
    RandomRDDs.normalVectorRDD(sc, nbr, length, partitionNbr, seed)
      .map(x => convert(x.toArray))
      .map(x => normalize(x).mkString(","))
      .zipWithUniqueId()
      .map { case (x, y) => (y, x) }
  }

  private def convert(ts: Array[Double]): Array[Float] = {
    val tn = ts.map(x => x.toFloat)
    for (i <- 1 to tn.length - 1) {
      tn(i) = tn(i - 1) + tn(i)
    }
    tn
  }

  private def normalize(ts: Array[Float]): Array[Float] = {
    val count = ts.length
    val mean = ts.sum / count
    val variance = ts.map(x => math.pow(x - mean, 2)).sum / (count - 1)
    val std = math.sqrt(variance)
    ts.map(x => ((x - mean) / std).toFloat)
  }

  def writeLog(content:String): Unit ={
    //Util.writeLog(content, true, rwCfg.logPath)
  }
}