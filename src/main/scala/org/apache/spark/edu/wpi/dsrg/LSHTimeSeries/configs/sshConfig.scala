package org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.configs

import org.apache.commons.lang.SerializationUtils
import org.apache.commons.math3.distribution._
import org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.utils.Util

import scala.collection.immutable.HashMap

/**
  * Created by lzhang6 on 9/15/17.
  */
object sshConfig extends AbsConfig {
  val w: Int = configMap("sshFilterLength").toInt
  val delta: Int = configMap("sshShiftSize").toInt
  val n: Int = getShingleLength()
  val r:Array[Double] = getR(w)

  val lshSamplingSize: Int = configMap("lshSamplingSize").toInt
  val lshBandNbr:Int = configMap("lshBandNbr").toInt
  val lshRowNbr = getLshRowNbr(lshSamplingSize,lshBandNbr)

  val saveDir: String = configMap("sshSavePath")

  val dictLength = 1
  val dict = HashMap.empty[Long,Int]

  val (rs, cs, betas) = getCwsSetting()

  private def getR(number:Int):Array[Double] = {
    val rSavePath = getPath("r")
    if(Util.hdfsDirExists(rSavePath)){
      val staff = Util.getArrayBytesFromHdfs(rSavePath)
      val data = SerializationUtils.deserialize(staff).asInstanceOf[Array[Double]]
      data
    }else{
      val rand = new scala.util.Random
      val tmp = 1 to number map { _ => rand.nextGaussian() }
      val data = tmp.toArray

      Util.writeArrayBytesToHdfs(rSavePath, SerializationUtils.serialize(data))
      Util.writeLog("==> save r parameters to %s".format(rSavePath), true, this.logPath)

      data
    }

  }

  private def getLshRowNbr(samplingSize:Int,lshRowNbr:Int): Int ={
    require(samplingSize%lshBandNbr == 0,"samplingSize%lshBandNbr != 0")
    samplingSize/lshRowNbr
  }

  def getCwsSetting(): (Array[Array[Double]], Array[Array[Double]], Array[Array[Double]]) = {
    val cwsSavePath = getPath("cws")

    if (Util.hdfsDirExists(cwsSavePath)) {
      val staff = Util.getArrayBytesFromHdfs(cwsSavePath)
      val data = SerializationUtils.deserialize(staff).asInstanceOf[(Array[Array[Double]], Array[Array[Double]], Array[Array[Double]])]
      data
    } else {
      val rs_t = distData(new GammaDistribution(2, 1))
      val cs_t = distData(new GammaDistribution(2, 1))
      val betas_t = distData(new UniformRealDistribution(0, 1))

      val data = (rs_t, cs_t, betas_t)
      Util.writeArrayBytesToHdfs(cwsSavePath, SerializationUtils.serialize(data))
      Util.writeLog("==> save cws parameters to %s".format(cwsSavePath), true, this.logPath)
      data
    }
  }

  def getPath(dataType:String):String={
    saveDir+"/"+dataType+"-"+w.toString+"-"+delta.toString+"-"+n.toString
  }

  def getShingleLength(): Int = {
    val temp = configMap("sshShingleLength").toInt
    require(temp <= 64, "only support 64 for shingle at most")
    temp
  }

  def generateClass(): SshCfg = {
    new SshCfg(
      this.w,
      this.delta,
      this.n,
      this.r,
      this.lshSamplingSize,
      this.rs,
      this.cs,
      this.betas,
      this.dict,
      this.dictLength
    )
  }

  override def printCfg(): Unit = Util.writeLog(this.toString, true, logPath)

  override def toString: String = {
    ("\n==> SSH algorithm Configuration" +
      "\n * filterLength\t%d" +
      "\n * shiftSize\t%d" +
      "\n * shingleLength\t%d" +
      "\n * random r\t%s" +
      "\n").format(
      w,
      delta,
      n,
      r.mkString(","))
  }

  private def distData(t: AbstractRealDistribution): Array[Array[Double]] = {

    val result = Array.ofDim[Double](lshSamplingSize, dictLength)
    for (s <- 0 until lshSamplingSize) {
      for (k <- 0 until dictLength) {
        result(s)(k) = t.sample()
      }
    }
    result
  }
}

class SshCfg(val w: Int,
             val delta: Int,
             val n: Int,
             val r: Array[Double],
             val samplingSize: Int,
             val rs: Array[Array[Double]],
             val cs: Array[Array[Double]],
             val betas: Array[Array[Double]],
             val dictMap: HashMap[Long, Int] ,
             val dictLength: Int
            )
