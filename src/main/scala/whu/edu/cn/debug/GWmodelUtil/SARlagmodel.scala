package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import scala.math._
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable.{ArrayBuffer, Map}
import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._
import whu.edu.cn.debug.GWmodelUtil.optimize._

class SARlagmodel extends SARmodels {

  var _xlength = 0
  var _xcol=0

  var _dX: DenseMatrix[Double] = _

  private var _1X: DenseMatrix[Double] = _
  private var _0X: DenseMatrix[Double] = _

  var lm_null: DenseVector[Double] = _
  var lm_w: DenseVector[Double] = _
  var _wy: DenseVector[Double] = _
  var _eigen: eig.DenseEig = _

  //  override def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
  //    geom = getGeometry(inputRDD)
  //  }

  override def setX(x: Array[DenseVector[Double]]) = {
    _X = x
    _xcol=x.length
    _xlength = _X(0).length
    _dX = DenseMatrix.create(rows = _xlength, cols = _X.length, data = _X.flatMap(t => t.toArray))
    val ones_x = Array(DenseVector.ones[Double](_xlength).toArray, x.flatMap(t => t.toArray))
    _1X = DenseMatrix.create(rows = _xlength, cols = x.length + 1, data = ones_x.flatten)
    _0X = _1X.copy
    _0X :+= -1.000
    _0X = _0X :+= 1e-12
  }

  override def setY(y: Array[Double]) = {
    _Y = DenseVector(y)
  }

  override def fit(): Unit = {

    val inte = getinterval()
    val rho = goldenSelection(inte._1, inte._2,function = rho4optimize)
    val yy = _Y - rho * _wy
    val betas = get_betas(X=_0X,Y = yy)
    val testbetas=get_betas(X=_0X)//lm.null
    val testres=get_res(X=_0X)//lm.null
    val res = get_res(X=_0X,Y = yy)
    val lly=get_logLik(get_res(X=_0X))
    println(lly)
    val llx = get_logLik(get_res(X=_0X,Y = yy))
    println(llx)
    val llrho=rho4optimize(rho)
    try_LRtest(llrho,lly)
    val fit = _Y - res
    val SSE = sum(res.toArray.map(t => t * t))
    val s2 = SSE / _xlength
    println(betas)
    println(res)
    calDiagnostic(X=_dX,Y = _Y,betas=betas,res=res,llrho)
//    calDiagnostic(X=_dX,Y = _Y,betas=testbetas,res=testres,lly)
    //    println(fit)
//    nelderMead(rho, betas)
  }

  def get_betas(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    betas
  }

  def get_res(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    Y - y_hat
  }

  def get_env(): Unit = {
    if (lm_null == null || lm_w == null || _wy == null) {
      _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
      lm_null = get_res(X=_0X)
      lm_w = get_res(X=_1X , Y = _wy)
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
  }

  def rho4optimize(rho: Double): Double = {
    get_env()
    println(lm_null)
    val e_a = lm_null.t * lm_null
    val e_b = lm_w.t * lm_null
    val e_c = lm_w.t * lm_w
    val SSE = e_a - 2.0 * rho * e_b + rho * rho * e_c
    val n = _xlength
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
    val eig_rho = eigvalue :*= rho
    val eig_rho_cp = eig_rho.copy
    val ldet = sum(breeze.numerics.log(-eig_rho_cp :+= 1.0))
    val ret = (ldet - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
//        println(ret)
    ret
  }

  def getinterval(): (Double, Double) = {
    get_env()
    val eigvalue = _eigen.eigenvalues.copy
    val min = eigvalue.toArray.min
    val max = eigvalue.toArray.max
    (1.0 / min, 1.0 / max)
  }

//  def goldenSelection(lower: Double, upper: Double, eps: Double = 1e-12): Double = {
//    var iter: Int = 0
//    val max_iter = 1000
//
//    val ratio: Double = (sqrt(5) - 1) / 2.0
//    var a = lower + 1e-12
//    var b = upper - 1e-12
//    var step = b - a
//    var p = a + (1 - ratio) * step
//    var q = a + ratio * step
//    var f_a = rho4optimize(a)
//    var f_b = rho4optimize(b)
//    var f_p = rho4optimize(p)
//    var f_q = rho4optimize(q)
//    //    println(f_a,f_b,f_p,f_q)
//    while (abs(f_a - f_b) >= eps && iter < max_iter) {
//      if (f_p > f_q) {
//        b = q
//        f_b = f_q
//        q = p
//        f_q = f_p
//        step = b - a
//        p = a + (1 - ratio) * step
//        f_p = rho4optimize(p)
//      } else {
//        a = p
//        f_a = f_p
//        p = q
//        f_p = f_q
//        step = b - a
//        q = a + ratio * step
//        f_q = rho4optimize(q)
//      }
//      iter += 1
//    }
//    println((b + a) / 2.0)
//    (b + a) / 2.0
//  }

}
