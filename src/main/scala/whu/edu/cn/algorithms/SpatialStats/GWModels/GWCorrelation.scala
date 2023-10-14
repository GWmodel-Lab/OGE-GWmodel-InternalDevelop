package whu.edu.cn.algorithms.SpatialStats.GWModels

import breeze.linalg.{*, DenseMatrix, DenseVector, MatrixSingularException, det, eig, inv, linspace, qr, sum, trace}
import scala.util.control.Breaks

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.math._
import whu.edu.cn.algorithms.SpatialStats.Utils.Optimize._

class GWCorrelation extends GWRbase {

  var _xrows = 0
  var _xcols = 0
  val select_eps = 1e-2

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

  def calAverage(bw: Double = 100, kernel: String = "gaussian", adaptive: Boolean = true) = {
    setweight(bw = 20, kernel = kernel, adaptive = adaptive)
    _X.map(t => {
      calCorrelationSerial(t, _X)
    })
  }

  def calCorrelationSerial(x: DenseVector[Double], arr: Array[DenseVector[Double]]): Unit = {
    //    setweight(bw = 20,kernel="bisquare",adaptive = true)
    val w_i = spweight_dvec.map(t => {
      val tmp = 1 / sum(t)
      t * tmp
    })
    val aLocalMean = w_i.map(w => w.t * x)
    println("aLocalMean")
    println(aLocalMean.toVector)
    val x_lm = aLocalMean.map(t => {
      x.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(x => x * x))
    val x_lm3 = x_lm.map(t => t.map(x => x * x * x))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
    val aStandardDev = aLVar.map(t => sqrt(t))
    val aLocalSkewness = w_ii.map(t => {
      (t._1.t * x_lm3(t._2)) / (aLVar(t._2) * aStandardDev(t._2))
    })
    val mLcv = DenseVector(aStandardDev) / DenseVector(aLocalMean)

    val corrSize=_xcols-1
    

  }

  def covwt(x1:DenseVector[Double], x2:DenseVector[Double], w:DenseVector[Double])= {
    val sqrtw=w.map(t=>sqrt(t))
    val re1= sqrtw * (x1- sum( x1 * w))
    val re2= sqrtw * (x2- sum( x2 * w))
    val sumww= - w.map(t=>t*t) + 1.0
    val re= re1 * re2 / sumww
  }


}