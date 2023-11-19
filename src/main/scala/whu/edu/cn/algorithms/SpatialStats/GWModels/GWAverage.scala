package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, linspace, qr, sum, trace}
import breeze.stats.median
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.util.control.Breaks
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

import scala.collection.mutable

class GWAverage extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  val select_eps = 1e-2

  private var _dX: DenseMatrix[Double] = _
  private var shpRDDidx: Array[((String, (Geometry, Map[String, Any])), Int)] = _

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(shpRDD.map(t => t._2._2(s).asInstanceOf[String].toDouble).collect())
    })
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _dX = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
  }

  override def setY(property: String): Unit = {
    _Y = DenseVector(shpRDD.map(t => t._2._2(property).asInstanceOf[String].toDouble).collect())
  }

  def findq(x: DenseVector[Double], w: DenseVector[Double], p: DenseVector[Double] = DenseVector(0.25, 0.50, 0.75)): DenseVector[Double] = {
    val lp = p.length
    val q = DenseVector(0.0, 0.0, 0.0)
    val x_ord = x.toArray.sorted
    val w_idx = w.toArray.zipWithIndex
    val x_w = w_idx.map(t => (x(t._2), t._1))
    val x_w_sort = x_w.sortBy(t => t._1)
    val w_ord = x_w_sort.map(t => t._2)
//    println(w_ord.toVector)
    val w_ord_idx = w_ord.zipWithIndex
    val cumsum = w_ord_idx.map(t => {
      w_ord.take(t._2 + 1).sum
    })
//    println(cumsum.toVector)
    for (j <- 0 until lp) {
      //找小于等于的，所以找大于的第一个，然后再减1，就是小于等于的最后一个
      val c_find = cumsum.find(_ > p(j))
      val c_first = c_find match {
        case Some(d) => d
        case None => 0.0
      }
      //减1
      var c_idx = cumsum.indexOf(c_first) - 1
      if (c_idx < 0) {
        c_idx = 0
      }
      q(j) = x_ord(c_idx)
//      println(s"q $j $q")
    }
    q
  }

  def calAverage(bw: Double = 0, kernel: String = "gaussian", adaptive: Boolean = true, quantile: Boolean = false): Unit = {
    setweight(bw = 20, kernel = kernel, adaptive = adaptive)
    shpRDDidx = shpRDD.collect().zipWithIndex
    shpRDDidx.foreach(t => t._1._2._2.clear())
    val Xidx = _X.zipWithIndex
    Xidx.foreach(t => {
      calAverageSerial(t._1, t._2, quantile)
    })
  }

  def calAverageSerial(x: DenseVector[Double], num: Int, quantile:Boolean =false): Unit = {
    val name = _nameX(num)
    var str="*                Results of Geographically Weighted Average                    *\n"
    print("*                Results of Geographically Weighted Average                    *\n")
    val w_i = spweight_dvec.map(t => {
      val tmp = 1 / sum(t)
      t * tmp
    })
    val aLocalMean = w_i.map(w => w.t * x)
    val m=median(DenseVector(aLocalMean))

    val x_lm = aLocalMean.map(t => {
      x.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(x => x * x))
    val x_lm3 = x_lm.map(t => t.map(x => x * x * x))
    //    x_lm2.map(t => t.foreach(println))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
//    val quantile = true
    if(quantile){
      val quant=w_i.map(t=>{
        findq(x,t)
      })
      val quant0= quant.map(t => {
        val tmp = t.toArray
        tmp(0)
      })
      val quant1 = quant.map(t => {
        val tmp = t.toArray
        tmp(1)
      })
      val quant2 = quant.map(t => {
        val tmp = t.toArray
        tmp(2)
      })
      println("calculate quantile value")
      val mLocalMedian = quant1
      val mIQR = DenseVector(quant2) - DenseVector(quant0)
      val mQI = ((2.0 * DenseVector(quant1)) - DenseVector(quant2) - DenseVector(quant0)) / mIQR
      shpRDDidx.map(t => {
        t._1._2._2 += (name + "_LMed" -> mLocalMedian(t._2))
        t._1._2._2 += (name + "_IQR" -> mIQR(t._2))
        t._1._2._2 += (name + "_QI" -> mQI(t._2))
      })
    }
    val aStandardDev = aLVar.map(t => sqrt(t))
    val aLocalSkewness = w_ii.map(t => {
      (t._1.t * x_lm3(t._2)) / (aLVar(t._2) * aStandardDev(t._2))
    })
    val mLcv = DenseVector(aStandardDev) / DenseVector(aLocalMean)
    shpRDDidx.map(t => {
      t._1._2._2 += (name + "_LM" -> aLocalMean(t._2))
      t._1._2._2 += (name + "_LVar" -> aLVar(t._2))
      t._1._2._2 += (name + "_LSD" -> aStandardDev(t._2))
      t._1._2._2 += (name + "_LSke" -> aLocalSkewness(t._2))
      t._1._2._2 += (name + "_LCV" -> mLcv(t._2))
    })
    print("*********************************************************************************\n")
  }


}