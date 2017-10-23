package org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.configs

/**
  * Created by leon on 9/1/17.
  */
import java.io.Serializable

import org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.utils.Util
import org.apache.spark.internal.Logging

abstract class AbsConfig extends Logging with Serializable{
  val configMap:Map[String, String] = Util.readConfigFile("./etc/config.conf")
  val logPath = configMap("logFileName")
  val blockSize = configMap("blockSize").toInt

  def printCfg():Unit
}