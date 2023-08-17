package whu.edu.cn.debug.GWmodelUtil.SpatialRegression

import breeze.linalg._
import scala.math._

import whu.edu.cn.debug.GWmodelUtil.optimize._

class SARlagmodel extends SARmodels {

  var _xrows = 0
  var _xcols = 0
  private var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _
  private var _lagY: DenseVector[Double]=_

  private var lm_null: DenseVector[Double] = _
  private var lm_w: DenseVector[Double] = _
  private var _wy: DenseVector[Double] = _
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

  def fit(): Array[Double] = {

    val interval = get_interval()
    val rho = goldenSelection(interval._1, interval._2, function = rho4optimize)
    _lagY = _Y - rho * _wy
    val betas = get_betas(X = _1X, Y = _lagY)
    val betas_map = betasMap(betas)
    val res = get_res(X = _1X, Y = _lagY)
    //log likelihood
    val lly = get_logLik(get_res(X = _1X))
    val llx = get_logLik(get_res(X = _1X, Y = _lagY))
    val llrho = rho4optimize(rho)

    fitvalue = (_Y - res).toArray
    println("---------------------------------spatial lag model---------------------------------")
    println(s"rho is $rho")
    try_LRtest(llrho, lly)
    println(s"coeffients:\n$betas_map")
    calDiagnostic(X = _dX, Y = _Y, residuals = res, loglikelihood = llrho, df = _df)
    println("------------------------------------------------------------------------------------")
    fitvalue
  }

  def get_betas(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xrows)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    betas
  }

  def get_res(X: DenseMatrix[Double] = _dX, Y: DenseVector[Double] = _Y, W: DenseMatrix[Double] = DenseMatrix.eye(_xrows)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    Y - y_hat
  }

  //添加一个判断X，Y为空的情况判断，抛出错误
  private def get_env(): Unit = {
    if (_Y != null && _X != null) {
      if (lm_null == null || lm_w == null || _wy == null) {
        _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
        lm_null = get_res(X = _1X)
        lm_w = get_res(X = _1X, Y = _wy)
      }
      if (spweight_dmat != null) {
        if (_eigen == null) {
          _eigen = breeze.linalg.eig(spweight_dmat.t)
        }
      } else {
        throw new NullPointerException("the shpfile is not initialized! please check!")
      }
    } else {
      throw new IllegalArgumentException("the x or y are not initialized! please check!")
    }
  }

  private def get_interval(): (Double, Double) = {
    if (spweight_dmat == null) {
      throw new NullPointerException("the shpfile is not initialized! please check!")
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
    val eigvalue = _eigen.eigenvalues.copy
    val min = eigvalue.toArray.min
    val max = eigvalue.toArray.max
    (1.0 / min, 1.0 / max)
  }

  private def rho4optimize(rho: Double): Double = {
    get_env()
    val e_a = lm_null.t * lm_null
    val e_b = lm_w.t * lm_null
    val e_c = lm_w.t * lm_w
    val SSE = e_a - 2.0 * rho * e_b + rho * rho * e_c
    val n = _xrows
    val s2 = SSE / n
    val eigvalue = _eigen.eigenvalues.copy
//    val eig_rho = eigvalue :*= rho
//    val eig_rho_cp = eig_rho.copy
    val ldet = sum(breeze.numerics.log(-eigvalue * rho + 1.0))
    val ret = (ldet - ((n / 2) * log(2 * math.Pi)) - (n / 2) * log(s2) - (1 / (2 * s2)) * SSE)
    ret
  }

//  private def goldenSelection(lower: Double, upper: Double, eps: Double = 1e-12): Double = {
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
