package whu.edu.cn.debug.GWmodelUtil.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, eig, inv, qr, sum}

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
    ones_x.map(t=>println(t.toVector))
  }

  override def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  def fit(): Unit = {
//    printweight()
//    val mat=SchurProduct(_1X,spweight_dvec(0))
//    println(mat)
    val xtw=spweight_dvec.map(w=> SchurProduct(_1X, w).t )
    val xtwx=xtw.map(t=>t * _1X)
    val xtwy=xtw.map(t=>t * _Y)
    val xtwx_inv=xtwx.map(t=>inv(t))
//    xtwy.foreach(println)
//    xtwx_inv.foreach(println)
    val xtwx_inv_idx=xtwx_inv.zipWithIndex
    val betas=xtwx_inv_idx.map(t=>t._1 * xtwy(t._2))
    betas.foreach(println)

  }

  def get_betas(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xrows)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    betas
  }

  def SchurProduct(Mat:DenseMatrix[Double],Vec:DenseVector[Double]): DenseMatrix[Double]={
    val arrbuf=new ArrayBuffer[DenseVector[Double]]()
    for(i<-0 until Mat.cols){
      arrbuf += Mat(::,i) * Vec
    }
    val data=arrbuf.toArray.map(t=>t.toArray).flatten
    DenseMatrix.create(rows = Mat.rows, cols = Mat.cols, data = data)
  }

}
