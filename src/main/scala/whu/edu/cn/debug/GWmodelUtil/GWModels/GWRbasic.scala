package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, qr, sum, trace}

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

  def fit(): Unit = {
//    printweight()
    val results=fitFunction(_dX,_Y,spweight_dvec)
    val betas=results._1
    val yhat=results._2
    val residual = results._3
    val shat=results._4
    println(yhat)
    println(residual)
    calDiagnostic(_dX,_Y,residual, shat)
    bandwidthSelection(adaptive = false,upper = max_dist)
  }

  def bandwidthSelection(kernel: String = "gaussian", adaptive: Boolean = true, upper:Double =max_dist ,lower:Double= max_dist/5000.0): Double = {
    _kernel = kernel
    _adaptive = adaptive
    var bw: Double = 0
    val d = 2.0
    try {
      bw = goldenSelection(lower, upper, eps = 1e-6, function = bandwidthAICc)
    } catch {
      case e: MatrixSingularException => {
        println("error")
        val low = lower * d
        bw = bandwidthSelection(_kernel, _adaptive,upper,low)
      }
    }
//    println(s"bw is $bw")
    bw
  }

  def bandwidthAICc(bw:Double):Double={
    setweight(bw,_kernel,_adaptive)
    val results = fitFunction(_dX, _Y, spweight_dvec)
    val residual = results._3
    val shat = results._4
    -getAICc(_dX, _Y, residual, shat)
  }

  private def fitFunction(X: DenseMatrix[Double]=_dX, Y: DenseVector[Double]=_Y, weight: Array[DenseVector[Double]] = spweight_dvec):
                 (Array[DenseVector[Double]], DenseVector[Double], DenseVector[Double], DenseMatrix[Double], Array[DenseVector[Double]])= {
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

  def getYhat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val arrbuf = new ArrayBuffer[Double]()
    for (i <- 0 until X.rows) {
      val rowvec = X(i, ::).inner
      val yhat = sum(betas(i) * rowvec)
      arrbuf += yhat
    }
    DenseVector(arrbuf.toArray)
  }

  def eachColProduct(Mat:DenseMatrix[Double],Vec:DenseVector[Double]): DenseMatrix[Double]={
    val arrbuf=new ArrayBuffer[DenseVector[Double]]()
    for(i<-0 until Mat.cols){
      arrbuf += Mat(::,i) * Vec
    }
    val data=arrbuf.toArray.flatMap(t=>t.toArray)
    DenseMatrix.create(rows = Mat.rows, cols = Mat.cols, data = data)
  }

}
