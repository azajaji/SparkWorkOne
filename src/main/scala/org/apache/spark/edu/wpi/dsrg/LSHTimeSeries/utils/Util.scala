package org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.utils

import java.io._
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

import scala.collection.mutable.HashMap
import scala.io.Source
import scala.math._

/**
  * Created by leon on 6/26/17.
  */
object Util extends Logging {

  def genZero(nbr: Int): String = {
    require(nbr >= 0, s"nbr>=0, but input is $nbr")
    if (nbr == 0) "" else "0" + genZero(nbr - 1)
  }

  def writeLog(content: String, isPrint: Boolean = true, logFileName: String): Unit = {
    try {
      val file = new File(logFileName)
      val writer = new PrintWriter(new FileOutputStream(file, true))

      val time = getCurrentTime()

      if (content.contains("Configuration")) {
        writer.append("\n" + "-*" * 15 + time + "-*" * 15 + "\n")
      }

      writer.append(time + content + "\n")
      if (isPrint) {
        println(time + content)
      }
      writer.close()
    } catch {
      case e: java.lang.NullPointerException => logError(e.toString)
    }
  }

  private[this] def getCurrentTime(): String = {
    val timeFormat = new SimpleDateFormat("HH:mm:ss - MM.dd.yyyy - z")
    val now = Calendar.getInstance().getTime()
    timeFormat.format(now)
  }

  def mean(xs: List[Long]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def stddev(xs: List[Long], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  def meanDouble(xs: List[Double]): Double = xs match {
    case Nil => 0.0
    case ys => ys.reduceLeft(_ + _) / ys.size.toDouble
  }

  def stddevDouble(xs: List[Double], avg: Double): Double = xs match {
    case Nil => 0.0
    case ys => math.sqrt((0.0 /: ys) {
      (a, e) => a + math.pow(e - avg, 2.0)
    } / xs.size)
  }

  //  def euclideanDistance(xs: Array[Double], ys: Array[Double]): Double = {
  //    sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
  //  }

  def euclideanDistance(xs: Array[Float], ys: Array[Float]): Double = {
    sqrt((xs zip ys).map { case (x, y) => pow(y - x, 2) }.sum)
  }




  def readConfigFile(configPath: String): Map[String, String] = {
    println("==> Read config from %s".format(configPath))

    var staff: Array[String] = null
    try {
      val configFile = Source.fromFile(configPath)
      staff = configFile.getLines().toArray
      configFile.close()
    } catch {
      case e1: FileNotFoundException => logError(configPath + e1.toString)
      case e2: IOException => logError(configPath + e2.toString)
        System.exit(0)
    }

    var content = HashMap.empty[String, String]
    val sep = "="
    for (line <- staff if (line.contains(sep) && !line.contains("#"))) {
      try {
        val t = line.split(sep)
        content += (t(0).trim -> t(1).trim)
      } catch {
        case e: Exception => println("==> %s can't parse %s".format(line.toString,e.toString))
      }
    }

    content.toMap
  }

  def checkDir(file: File): Unit = {
    val dir = new File(file.getParent)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  def hdfsDirExists(hdfsDirectory: String): Boolean = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    val exists = try {
      fs.exists(new Path(hdfsDirectory))
    } catch {
      case e: java.net.ConnectException => {
        logError(e.toString)
        false
      }
    }

    return exists
  }

  def removeHdfsFiles(hdfsDirectory: String): String = {
    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    try {
      fs.delete(new Path(hdfsDirectory), true)
    } catch {
      case e: Exception => logError(e.toString)
    }

    "==> remove %s".format(hdfsDirectory)
  }

  def getHdfsFileNameList(hdfsFileName: String): List[String] = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())
    fs.listStatus(new Path(hdfsFileName)).map {
      _.getPath.toString
    }.filter(x => !x.contains("_SUCCESS")).toList
  }

  def writeArrayBytesToHdfs(hdfsFileName: String, data: Array[Byte]): Unit = {
    val fs = FileSystem.get(new URI(hdfsFileName), new Configuration())

    if (Util.hdfsDirExists(hdfsFileName))
      Util.removeHdfsFiles(hdfsFileName)

    val os = fs.create(new Path(hdfsFileName))
    try {
      os.write(data)
    } finally {
      os.close()
    }
  }

  def getHdfsFileSizeMList(hdfsDirectory: String): List[Double] = {
    val hadoopConf = new Configuration()
    val uri = new URI(hdfsDirectory)
    val fListStatus = FileSystem.get(uri, hadoopConf).listStatus(new Path(hdfsDirectory))
    fListStatus.map { x => convertToM(x.getLen) }.filter(x => x != 0.0).toList
  }

  def getArrayBytesFromHdfs(hdfsDirectory: String): Array[Byte] = {
    require(hdfsDirExists(hdfsDirectory), "%s doesn't exist".format(hdfsDirectory))

    val fs = FileSystem.get(new URI(hdfsDirectory), new Configuration())
    val path = new Path(hdfsDirectory)
    val is = fs.open(path)
    val fileSize = fs.listStatus(path).map {
      _.getLen
    }.max.toInt

    var contentBuffer = new Array[Byte](fileSize)
    is.readFully(0, contentBuffer)
    require((fileSize == contentBuffer.size), "(fileSize: %d == contentBuffer.size: %d)".format(fileSize, contentBuffer.size))
    is.close()
    contentBuffer
  }

  def fetchBoolFromString(input: String): Boolean = {
    val content = input.trim.toLowerCase
    if (content == "true" || content == "yes") true
    else false
  }

  private def convertToM(size: Long): Double = {
    (size * 1.0) / (1024 * 1024)
  }
}