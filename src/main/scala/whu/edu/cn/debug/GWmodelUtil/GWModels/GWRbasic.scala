package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, det, eig, inv, qr, sum, trace}

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._

class GWRbasic extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _

  private var _eigen: eig.DenseEig = _

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
//    val mat=SchurProduct(_1X,spweight_dvec(0))
//    println(mat)
    val xtw=spweight_dvec.map(w=> eachColProduct(_1X, w).t )
    val xtwx=xtw.map(t=>t * _1X)
    val xtwy=xtw.map(t=>t * _Y)
    val xtwx_inv=xtwx.map(t=>inv(t))
//    xtwy.foreach(println)
//    xtwx_inv.foreach(println)
    val xtwx_inv_idx=xtwx_inv.zipWithIndex
    val betas=xtwx_inv_idx.map(t=>t._1 * xtwy(t._2))
//    betas.foreach(println)
    val ci=xtwx_inv_idx.map(t=>t._1 * xtw(t._2))
    val ci_idx=ci.zipWithIndex
    val sum_ci=ci.map(t=>t.map(t=>t*t)).map(t=>sum(t(*,::)))
//    sum_ci.foreach(println)
    val si=ci_idx.map(t=> (_1X(t._2,::).inner.t * t._1).inner)
//    si.foreach(println)
    val shat=DenseMatrix.create(rows = si.length, cols = si(0).length, data = si.flatMap(t=>t.toArray))
    val shat0=trace(shat)
    println(shat0)
    val shat1=trace(shat * shat.t)
    println(shat1)
    val yhat=getYhat(_1X,betas)
    val residual = _Y - getYhat(_1X, betas)
    println(yhat)
    println(residual)
    calDiagnostic(_1X,_Y,residual, shat)
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
