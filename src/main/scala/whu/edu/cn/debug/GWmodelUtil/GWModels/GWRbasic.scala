package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, det, eig, inv, qr, sum, trace}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.debug.GWmodelUtil.Utils.Optimize._

class GWRbasic extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _

  override def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    _dX = DenseMatrix.create(rows = _xrows, cols = _X.length, data = _X.flatMap(t => t.toArray))
    val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, x.flatMap(t => t.toArray))
    _1X = DenseMatrix.create(rows = _xrows, cols = x.length + 1, data = ones_x.flatten)
    _df = _xcols + 1 + 1
  }

  override def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  def fit(): Unit = {
//    printweight()
    val results=fitFunction(_1X,_Y,spweight_dvec)
    val betas=results._1
    val yhat=results._2
    val residual = results._3
    val shat=results._4
    println(yhat)
    println(residual)
    calDiagnostic(_1X,_Y,residual, shat)
//    bandwidthSelection(adaptive = false)
  }

  def bandwidthSelection( kernel: String = "gaussian", adaptive:Boolean=true)={
    _kernel = kernel
    _adaptive = adaptive
    val bw=goldenSelection(2000,27608,eps= 1e-6,function = bandwidthAICc)
    println(s"bw is $bw")
  }

  def bandwidthAICc(bw:Double):Double={
    setweight(bw,_kernel,_adaptive)
    val results = fitFunction(_1X, _Y, spweight_dvec)
    val residual = results._3
    val shat = results._4
    -getAICc(_1X, _Y, residual, shat)
  }

  private def fitFunction(X: DenseMatrix[Double]=_1X, Y: DenseVector[Double]=_Y, weight: Array[DenseVector[Double]] = spweight_dvec):
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
