package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg.{DenseMatrix, DenseVector, eig, qr}
import breeze.numerics.sqrt
import scala.math.log
import scala.collection.mutable.{ArrayBuffer, Map}

class SARerrormodel extends SARmodels {

  var _xrows = 0
  var _xcols = 0
  var _df = _xcols

  private var _dX: DenseMatrix[Double] = _
  private var _1X: DenseMatrix[Double] = _


  private var sum_lw:Double=_
  private var sw: DenseVector[Double] = _
  private var _wy: DenseVector[Double] = _
  private var _wx: DenseMatrix[Double] = _
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

  def get_env()= {
    if (_wy == null) {
      _wy = DenseVector(spweight_dvec.map(t => (t dot _Y)))
    }
    if (_eigen == null) {
      _eigen = breeze.linalg.eig(spweight_dmat.t)
    }
    if(sum_lw==null || sw == null) {
      val weight1: DenseVector[Double] = DenseVector.ones[Double](_xrows)
      sum_lw = weight1.toArray.map(t => log(t)).sum
      sw = sqrt(weight1)
    }
    if(_wx==null){
      val _dvecWx=_X.map(t=>DenseVector(spweight_dvec.map(i => (i dot t))))
      val _dmatWx=DenseMatrix.create(rows = _xrows, cols = _dvecWx.length, data = _dvecWx.flatMap(t => t.toArray))
      val ones_x = Array(DenseVector.ones[Double](_xrows).toArray, _dvecWx.flatMap(t => t.toArray))
      _wx = DenseMatrix.create(rows = _xrows, cols = _dvecWx.length + 1, data = ones_x.flatten)
    }
//    println(_wy)
//    println(s"-----------\n$sum_lw\n$sw")
//    println(_wx)
  }

  def lambda4optimize(lambda:Double)={
    get_env()
    val yl= sw * (_Y - lambda * _wy)
    val xl= (_1X - lambda * _wx)
    val xl_qr = qr(xl)
    val xl_qr_q=xl_qr.q(::,0 to _xcols)//列数本来应该+1，由于从0开始计数，反而刚好合适
//    println(xl_qr_q)
    val xl_q_yl= xl_qr_q.t * yl
    val SSE=yl.t*yl-xl_q_yl.t*xl_q_yl
    println(SSE)
  }


}
