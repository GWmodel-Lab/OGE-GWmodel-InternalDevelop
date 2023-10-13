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

  def calAverageSerial(): Unit = {
    setweight(bw = 20,kernel="bisquare",adaptive = true)
    val w_i = spweight_dvec.map(t=>{
      val tmp = 1/sum(t)
      t * tmp
    })
    val ix=_X(0)
    val aLocalMean=w_i.map(w=>w.t * ix)
//    val aLocalMean=w_i.map(w=>(w.t * _dX).inner)
////    aLocalMean.foreach(println)
//    val mlocalmean = DenseMatrix.create(rows = aLocalMean.length, cols = aLocalMean(0).length, data = aLocalMean.flatMap(t => t.toArray))
//    val tlocalmean=mlocalmean(::,0)
    println(aLocalMean.toVector)
//    calLocalVar(DenseVector(aLocalMean),_X,w_i)

    val x_lm = aLocalMean.map(t => {
      ix.map(i => {
        i - t
      })
    })
    val x_lm2 = x_lm.map(t => t.map(x => x * x))
    val x_lm3 = x_lm.map(t => t.map(x => x * x * x))
    //    x_lm2.map(t => t.foreach(println))
    val w_ii = w_i.zipWithIndex
    val aLVar = w_ii.map(t => {
      t._1.t * x_lm2(t._2)
    })
    println("aLVar")
    println(aLVar.toVector)
    val aStandardDev=aLVar.map(t=>sqrt(t))
    println(aStandardDev.toVector)
    val aLocalSkewness=w_ii.map(t => {
      (t._1.t * x_lm3(t._2)) / (aLVar(t._2) * aStandardDev(t._2))
    })
    println(aLocalSkewness.toVector)
    val mlcv=DenseVector(aStandardDev) / DenseVector(aLocalMean)
    println(mlcv)
  }


}