package org.apache.spark.edu.wpi.dsrg.LSHTimeSeries.utils

import org.apache.commons.math3.distribution.{AbstractRealDistribution, GammaDistribution, UniformRealDistribution}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by ajaji on 10/11/17.
  */
object SshTransform {

  val w: Int = SshParams.w
  val delta: Int = SshParams.delta
  val n: Int = SshParams.n

  var r: Array[Double] = SshParams.r

  val samplingSize: Int = SshParams.samplingSize

  val dictLength = SshParams.dictLength

  val rs_t = SshParams.rs_t
  val cs_t = SshParams.cs_t
  val betas_t = SshParams.betas_t


  def main(args: Array[String]): Unit = {

    //EXAMPLE:
    //r = SshParams.getR(w)
    //val sketched2 = sketch(SshParams.ts2, w, delta, r) // 10 in dataset
    //val shingled2 = shingle(sketched2)
    //val wM2 = weightedMinHash(shingled2)

  }




  def byteAry2Long(value: Array[Byte]): String = {
    val str = value.mkString("")
    Integer.parseInt(str, 2).toString
  }


  def shingle(bTs: Array[Byte]): Array[Int] = {
    val btSet = bTs.sliding(n, 1).map(x => byteAry2Long(x)).toArray
    val weightSet = set2WeightSet(btSet)

    weightSet.map {case (x, y) => y}
  }


  def sketch(ts: Array[Double], w: Int, delta: Int, r: Array[Double]): Array[Byte] = {
    val tsSegs = ts.sliding(w, delta)
    tsSegs.map { x => sign(dotProduct(x, r)) }.toArray
  }


  private def dotProduct(a: Array[Double], b: Array[Double]): Double = {
    val bp = if (a.length < b.length) b.take(a.length) else b
    (a zip bp).map { case (x, y) => x * y }.sum
  }

  private def sign(x: Double): Byte = if (x >= 0.0) 1 else 0


  private def getR(number: Int): Array[Float] = {
    /* val rSavePath = getPath("r")
     if(Util.hdfsDirExists(rSavePath)){
       val staff = Util.getArrayBytesFromHdfs(rSavePath)
       val data = SerializationUtils.deserialize(staff).asInstanceOf[Array[Float]]
       data
     }else{*/
    val rand = new scala.util.Random
    val tmp = 1 to number map { _ => rand.nextFloat() }
    val data = tmp.toArray

    //Util.writeArrayBytesToHdfs(rSavePath, SerializationUtils.serialize(data))
    //Util.writeLog("==> save r parameters to %s".format(rSavePath), true, this.logPath)

    data
  }


  def weightedMinHash(ngram: Array[Int]): Array[(Int, Int)] = {


    //SshParams.updateCoefficents(ngram.length)


    val result = ArrayBuffer.empty[(Int, Int)]

    var kk = 0
    for (s <- 0 until samplingSize) {
      val tks = Array.fill[Double](dictLength)(0)
      val yks = Array.fill[Double](dictLength)(0)
      val aks = Array.fill[Double](dictLength)(0)



      for (k <- 0 until ngram.length if ngram(k) != 0) {
        //print ("RS:")
        //rs_t(s).foreach(println)
        val tk = math.floor(math.log(ngram(k)) / rs_t(s)(k) + betas_t(s)(k))
        val yk = math.exp(rs_t(s)(k) * (tk - betas_t(s)(k)))
        val ak = cs_t(s)(k) / (yk * math.exp(rs_t(s)(k)))

        tks(k) = tk
        yks(k) = yk
        aks(k) = ak
        kk = k

      }



      val k_s = argMinIndex(aks)
      //result += ((k_s, yks(k_s).toInt))
      //result += ((k_s, tks(k_s).toInt))
      result += ((k_s,k_s))
    }
    //println("k is " + kk)

    result.toArray
  }

  def calcJaccard(x: Array[(Int, Int)], y: Array[(Int, Int)]): Float = {
    val intersection = 0

    val count = x.map {
      x => y.contains(x)
    }.count(_ == true)

    val sim: Float =  (count.toFloat / x.length)
    //println("Intersection: " + sim)

    return sim
  }


  def set2WeightSet(value: Array[String]): Array[(String, Int)] = {
    val tempMap = mutable.HashMap.empty[String, Int]
    for (v <- value) {
      if (tempMap.contains(v)) {
        tempMap(v) = tempMap(v) + 1
      } else {
        tempMap += (v -> 1)
      }
    }
    tempMap.toArray
  }


  private def nGram(value: Array[(String, Int)], length: Int): Array[Int] =  {


      //println("using length"+length.toString)

      //value.foreach(println)

      val result = ArrayBuffer()
    var i = 0
    for ((idx, nbr) <- value) {
      //val new_index = SshDictionary.get(idx)
      if (idx != -1) {
        result.insert(nbr)
        i += 1
      }
    }
    result.toArray[Int]
    }

    private def argMinIndex(value: Array[Double]): Int = {
      var min = 0.0
      for (k <- value if k != 0) {
        if (k < min || min == 0) min = k
      }

      value.indexOf(min)
    }



}
