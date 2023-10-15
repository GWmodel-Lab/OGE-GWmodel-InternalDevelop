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

  def calCorrelation(bw: Double = 100, kernel: String = "gaussian", adaptive: Boolean = true) = {
    setweight(bw = 20, kernel = "bisquare", adaptive = adaptive)
//    _X.map(t => {
//      calCorrelationSerial(t, _X)
//    })

    val x1 = _X(0)
    val x2 = _X(1)

    val w_i = spweight_dvec.map(t => {
      val tmp = 1 / sum(t)
      t * tmp
    })
//    val sum_wi2=w_i.map(t1=>sum(t1.map(t2=>t2*t2)))
    val aLocalMean = w_i.map(w => w.t * x1)
    println("aLocalMean")
    println(aLocalMean.toVector)
    val x_lm = aLocalMean.map(t => {
      x1.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(i => i * i))
    val x_lm3 = x_lm.map(t => t.map(i => i * i * i))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
    val aStandardDev = aLVar.map(t => sqrt(t))
    val aLocalSkewness = w_ii.map(t => {
      (t._1.t * x_lm3(t._2)) / (aLVar(t._2) * aStandardDev(t._2))
    })
    val mLcv = DenseVector(aStandardDev) / DenseVector(aLocalMean)

    val aLocalMean2 = w_i.map(w => w.t * x2)
    val x2_lm = aLocalMean2.map(t => {
      x2.map(i => {
        i - t
      })
    })
    val x2_lm2 = x2_lm.map(t => t.map(i => i * i))
    val aLVar2 = w_ii.map(t => {
      t._1.t * x2_lm2(t._2)
    })

    val corrSize = _xcols - 1

    val covmat=w_i.map(t=>{
      covwt(x1,x2,t)
    })
    val corrmat= w_ii.map(t => {
      val sum_wi2=sum(t._1.map(i=>i*i))
      val covjj = aLVar(t._2) / (1.0 - sum_wi2)
      val covkk = aLVar2(t._2) / (1.0 - sum_wi2)
      covwt(x1, x2, t._1) / sqrt(covjj * covkk)
    })

    covmat.foreach(println)
    println(corrmat.toVector)
  }

  def covwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double  = {
    val sqrtw = w.map(t => sqrt(t))
    val re1 = sqrtw * (x1 - sum(x1 * w))
    val re2 = sqrtw * (x2 - sum(x2 * w))
    val sumww = - sum(w.map(t => t * t)) + 1.0
    sum(re1 * re2 * (1 / sumww))
  }

  def corwt(x1: DenseVector[Double], x2: DenseVector[Double], w: DenseVector[Double]): Double = {
    covwt(x1, x2, w) / covwt(x1, x1, w) * covwt(x2, x2, w)
  }

}