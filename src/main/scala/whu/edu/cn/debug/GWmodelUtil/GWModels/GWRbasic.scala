package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.debug.GWmodelUtil.Utils.Optimize._

class GWRbasic extends GWRbase {

  var _xrows = 0
  var _xcols = 0

  private var _dX: DenseMatrix[Double] = _

  override def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _dX = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
  }

  override def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  def fit(bw:Double= 0, kernel: String="gaussian", approach: String = "AICc", adaptive: Boolean = true): Unit = {
    if(bw > 0){
      setweight(bw, kernel, adaptive)
    }else if(spweight_dvec!=null){

    }else{
      throw new IllegalArgumentException("bandwidth should be over 0 or spatial weight should be initialized")
    }
    //    printweight()
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val betas = results._1
    val yhat = results._2
    val residual = results._3
    val shat = results._4
    println(yhat)
    println(residual)
    calDiagnostic(_dX, _Y, residual, shat)
//    val bwselect = bandwidthSelection(kernel = "bisquare", approach = "CV", adaptive = false)
//    println(bwselect)

    //    setweight(bw= bw,kernel = _kernel, adaptive = false)
    //    val results2=fitFunction(_dX,_Y,spweight_dvec)
    //    calDiagnostic(_dX,_Y,results2._3, results2._4)
  }

  private def fitFunction(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, weight: Array[DenseVector[Double]] = spweight_dvec):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseVector[Double]]) = {
    val xtw = weight.map(w => eachColProduct(X, w).t)
    val xtwx = xtw.map(t => t * X)
    val xtwy = xtw.map(t => t * Y)
    val xtwx_inv = xtwx.map(t => inv(t))
    val xtwx_inv_idx = xtwx_inv.zipWithIndex
    val betas = xtwx_inv_idx.map(t => t._1 * xtwy(t._2))
    val ci = xtwx_inv_idx.map(t => t._1 * xtw(t._2))
    val ci_idx = ci.zipWithIndex
    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => (X(t._2, ::).inner.t * t._1).inner)
    val shat = DenseMatrix.create(rows = si.length, cols = si(0).length, data = si.flatMap(t => t.toArray))
    val yhat = getYhat(X, betas)
    val residual = Y - getYhat(X, betas)
    //是不是可以用一个struct来存
    (betas, yhat, residual, shat, sum_ci)
  }

  private def fitRDDFunction(implicit sc: SparkContext, X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, weight: Array[DenseVector[Double]] = spweight_dvec):
  (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseVector[Double]]) = {
    val xtw = sc.makeRDD(weight.map(w => eachColProduct(X, w).t))
    val xtwx = xtw.map(t => t * X)
    val xtwy = xtw.map(t => t * Y)
    val xtwx_inv = xtwx.map(t => inv(t))
    val xtwx_inv_idx = xtwx_inv.zipWithIndex
    val betas = xtwx_inv_idx.map(t => t._1 * xtwy.collect()(t._2.toInt))
    val ci = xtwx_inv_idx.map(t => t._1 * xtw.collect()(t._2.toInt))
    val ci_idx = ci.zipWithIndex
    val sum_ci = ci.map(t => t.map(t => t * t)).map(t => sum(t(*, ::)))
    val si = ci_idx.map(t => (X(t._2.toInt, ::).inner.t * t._1).inner)
    val shat = DenseMatrix.create(rows = si.collect().length, cols = si.collect()(0).length, data = si.collect().flatMap(t => t.toArray))
    val yhat = getYhat(X, betas.collect())
    val residual = Y - getYhat(X, betas.collect())
    //是不是可以用一个struct来存
    (betas.collect(), yhat, residual, shat, sum_ci.collect())
  }

  def bandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", adaptive: Boolean = true): Double = {
    if (adaptive) {
      adaptiveBandwidthSelection(kernel = kernel, approach = approach)
    } else {
      fixedBandwidthSelection(kernel = kernel, approach = approach)
    }
  }

  private def fixedBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Double = max_dist, lower: Double = max_dist / 5000.0): Double = {
    _kernel = kernel
    _adaptive = false
    var bw: Double = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      bw = goldenSelection(lower, upper, eps = 1e-4, findMax = false, function = approachfunc)
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 1.618
        bw = fixedBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def adaptiveBandwidthSelection(kernel: String = "gaussian", approach: String = "AICc", upper: Int = _xrows - 1, lower: Int = 20): Int = {
    _kernel = kernel
    _adaptive = true
    var bw: Int = lower
    var approachfunc: Double => Double = bandwidthAICc
    if (approach == "CV") {
      approachfunc = bandwidthCV
    }
    try {
      bw = round(goldenSelection(lower, upper, eps = 1e-4, findMax = false, function = approachfunc)).toInt
    } catch {
      case e: MatrixSingularException => {
        //        println("error")
        val low = lower + 1
        bw = adaptiveBandwidthSelection(kernel, approach, upper, low)
      }
    }
    bw
  }

  private def bandwidthAICc(bw: Double): Double = {
    if (_adaptive) {
      setweight(round(bw), _kernel, _adaptive)
    } else {
      setweight(bw, _kernel, _adaptive)
    }
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val residual = results._3
    val shat = results._4
    val shat0 = trace(shat)
    val rss = residual.toArray.map(t => t * t).sum
    val n = _xrows
    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
  }

  private def bandwidthCV(bw: Double): Double = {
    if (_adaptive) {
      setweight(round(bw), _kernel, _adaptive)
    } else {
      setweight(bw, _kernel, _adaptive)
    }
    val spweight_idx = spweight_dvec.zipWithIndex
    spweight_idx.map(t => t._1(t._2) = 0)
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val residual = results._3
    residual.toArray.map(t => t * t).sum
  }

  def getYhat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val arrbuf = new ArrayBuffer[Double]()
    for (i <- 0 until X.rows) {
      val rowvec = X(i, ::).inner
      val yhat = sum(betas(i) * rowvec)
      arrbuf += yhat
    }
    DenseVector(arrbuf.toArray)
  }

//  def getAICc(residual: DenseVector[Double], shat: DenseMatrix[Double]): Double = {
//    val shat0 = trace(shat)
//    val rss = residual.toArray.map(t => t * t).sum
//    val n = _xrows
//    n * log(rss / n) + n * log(2 * math.Pi) + n * ((n + shat0) / (n - 2 - shat0))
//  }

  private def eachColProduct(Mat: DenseMatrix[Double], Vec: DenseVector[Double]): DenseMatrix[Double] = {
    val arrbuf = new ArrayBuffer[DenseVector[Double]]()
    for (i <- 0 until Mat.cols) {
      arrbuf += Mat(::, i) * Vec
    }
    val data = arrbuf.toArray.flatMap(t => t.toArray)
    DenseMatrix.create(rows = Mat.rows, cols = Mat.cols, data = data)
  }

}