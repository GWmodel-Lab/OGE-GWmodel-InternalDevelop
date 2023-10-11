package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, linspace, qr, sum, trace}
import scala.util.control.Breaks

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

class GWAverage extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  val select_eps=1e-2

  private var _dX: DenseMatrix[Double] = _

  override def setX(x: Array[DenseVector[Double]]): Unit = {
    _X = x
    _xcols = x.length
    _xrows = _X(0).length
    _dX = DenseMatrix.create(rows = _xrows, cols = _xcols, data = _X.flatMap(t => t.toArray))
  }

  override def setY(y: Array[Double]): Unit = {
    _Y = DenseVector(y)
  }

  def findq(x: Array[Double], w: DenseVector[Double], p: DenseVector[Double] = DenseVector(0.25, 0.50, 0.75)): DenseVector[Double] = {
    val lp = p.length
    val q = DenseVector(0.0, 0.0, 0.0)
    val x_ord = x.sorted
    val w_idx = w.toArray.zipWithIndex
    val x_w = w_idx.map(t => (x(t._2), t._1))
    val x_w_sort = x_w.sortBy(t => t._1)
    val w_ord = x_w_sort.map(t => t._2)
    println(w_ord.toVector)
    val w_ord_idx = w_ord.zipWithIndex
    val cumsum = w_ord_idx.map(t => {
      w_ord.take(t._2 + 1).sum
    })
    println(cumsum.toVector)

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
      println(s"q $j $q")
    }
    q
  }

  def calAverage(): Unit = {
    setweight(bw = 20,kernel="bisquare",adaptive = true)
    val w_i = spweight_dvec.map(t=>{
      val tmp = 1/sum(t)
      t * tmp
    })
    val alocalmean=w_i.map(w=>(w.t * _dX).inner)
//    alocalmean.foreach(println)
    val mlocalmean = DenseMatrix.create(rows = alocalmean.length, cols = alocalmean(0).length, data = alocalmean.flatMap(t => t.toArray))
//    println(mlocalmean)
    val center=_dX - mlocalmean
    val center2=center.map(t=>t*t)

    val almidx=alocalmean.zipWithIndex
    val cent=almidx.map(t=>{
      _X(0)(t._2)-t._1
    })
    cent.foreach(println)
    val mcent=DenseMatrix.create(rows = cent.length, cols = cent(0).length, data = cent.flatMap(t => t.toArray))
    val lvar= w_i.map(t => {
      (t.t * mcent).inner
    })
    println("lvar")
    lvar.foreach(println)

    val mlvar= w_i.map(t => {
      (t.t * center2).inner
    })
    println("mlvar")
    mlvar.foreach(println)
    val mstand=mlvar.map(t=>t.map(i=>sqrt(i)))
    mstand.foreach(println)
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
    val si = ci_idx.map(t => {
      val a=X(t._2, ::).inner.toDenseMatrix
      val b=t._1.toDenseMatrix
      a * b
      //      (X(t._2, ::) * t._1).inner
    })
    val shat = DenseMatrix.create(rows = si.length, cols = si.length, data = si.flatMap(t => t.toArray))
    val yhat = getYHat(X, betas)
    val residual = Y - yhat
    //是不是可以用一个struct来存
    (betas, yhat, residual, shat, sum_ci)
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
      bw = goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)
    } catch {
      case e: MatrixSingularException => {
        val low = lower * 2
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
      bw = round(goldenSelection(lower, upper, eps = select_eps, findMax = false, function = approachfunc)).toInt
    } catch {
      case e: MatrixSingularException => {
        println("error")
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

  def getYHat(X: DenseMatrix[Double], betas: Array[DenseVector[Double]]): DenseVector[Double] = {
    val betas_idx=betas.zipWithIndex
    val yhat=betas_idx.map(t=>{
      sum(t._1 * X(t._2,::).inner)
    })
    DenseVector(yhat)
  }

  private def eachColProduct(Mat: DenseMatrix[Double], Vec: DenseVector[Double]): DenseMatrix[Double] = {
    val arrbuf = new ArrayBuffer[DenseVector[Double]]()
    for (i <- 0 until Mat.cols) {
      arrbuf += Mat(::, i) * Vec
    }
    val data = arrbuf.toArray.flatMap(t => t.toArray)
    DenseMatrix.create(rows = Mat.rows, cols = Mat.cols, data = data)
  }

}