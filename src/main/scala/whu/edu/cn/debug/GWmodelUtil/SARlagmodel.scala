package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import scala.math._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.{ArrayBuffer, Map}
import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

class SARlagmodel extends SARmodels {

  var _betas: DenseVector[Double]= _

  var _xlength=0
  var _dX:DenseMatrix[Double] = null

  var lm_null: DenseVector[Double]=_
  var lm_w: DenseVector[Double]=_
  var _wy:DenseVector[Double] = null
  var _eigen: eig.DenseEig=_

//  override def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
//    geom = getGeometry(inputRDD)
//  }

  override def setX(x: Array[DenseVector[Double]]) = {
    _X = x
    _xlength = _X(0).length
    _dX =DenseMatrix.create(rows=_xlength,cols=_X.length,data = _X.flatMap(t=>t.toArray))
  }

  override def setY(y: Array[Double]) = {
    _Y = DenseVector(y)
  }

  override def fit(): Unit = {
    println("created and override")
  }

//  def get_res(W:DenseMatrix[Double] = DenseMatrix.eye(xlength)): DenseVector[Double] ={
//    val xtw = _dX.t * W
//    val xtwx = xtw * _dX
//    val xtwy = xtw * _Y
//    val xtwx_inv = inv(xtwx)
//    val betas0 = xtwx_inv * xtwy
//    val y_hat = _dX * betas0
//    _Y - y_hat
//  }

  def specify_res(X: DenseMatrix[Double]= _dX , Y: DenseVector[Double]= _Y , W: DenseMatrix[Double] = DenseMatrix.eye(_xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas0 = xtwx_inv * xtwy
    val y_hat = X * betas0
    Y - y_hat
  }

  def get_env(): Unit = {
    if (lm_null == null || lm_w == null || _wy == null) {
      _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
      //      wy.foreach(println)
      lm_null = specify_res()
      lm_w = specify_res(Y = _wy)
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
  }

  def func4optimize(rho: Double): Double = {
    get_env()
    val e_a = lm_null.t * lm_null
    val e_b = lm_w.t * lm_null
    val e_c = lm_w.t * lm_w
    val SSE = e_a - 2.0 * rho * e_b + rho * rho * e_c
    val n = _xlength
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    val eig_rho = eigvalue :*= rho
    val eig_rho_cp = eig_rho.copy
    val ldet = sum(breeze.numerics.log(- eig_rho_cp :+= 1.0))
    val ret = (ldet - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
//    println(ret)
    ret
  }

  def getinterval():(Double,Double)={
    val eigvalue = _eigen.eigenvalues.copy
    val min= eigvalue.toArray.min
    val max= eigvalue.toArray.max
    (1.0/min, 1.0/max)
  }

  def goldenSelection(lower: Double, upper: Double, eps: Double = 1e-10): Double = {
    var iter: Int = 0
    val max_iter = 1000

    val ratio: Double = (sqrt(5) - 1) / 2.0
    var a = lower + 1e-12
    var b = upper - 1e-12
    var step = b - a
    var p = a + (1 - ratio) * step
    var q = a + ratio * step
    var f_a = func4optimize(a)
    var f_b = func4optimize(b)
    var f_p = func4optimize(p)
    var f_q = func4optimize(q)
    //    println(f_a,f_b,f_p,f_q)
    while (abs(f_a - f_b) >= eps && iter < max_iter) {
      if (f_p > f_q) {
        b = q
        f_b = f_q
        q = p
        f_q = f_p
        step = b - a
        p = a + (1 - ratio) * step
        f_p = func4optimize(p)
      } else {
        a = p
        f_a = f_p
        p = q
        f_p = f_q
        step = b - a
        q = a + ratio * step
        f_q = func4optimize(q)
      }
      iter += 1
    }
    println((b + a)/2.0)
    (b + a) / 2.0
  }

}
