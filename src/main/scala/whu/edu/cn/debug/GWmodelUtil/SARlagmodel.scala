package whu.edu.cn.debug.GWmodelUtil

import breeze.linalg._
import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import scala.collection.mutable.{ArrayBuffer, Map}

import whu.edu.cn.debug.GWmodelUtil.GWMdistance._
import whu.edu.cn.debug.GWmodelUtil.GWMspatialweight._

class SARlagmodel extends SARmodels {

  var betas: DenseVector[Double]= _
  var xlength=0
  var _dX:DenseMatrix[Double] = null

  var lm_null: DenseVector[Double]=_
  var lm_w: DenseVector[Double]=_

//  def init(inputRDD: RDD[(String, (Geometry, Map[String, Any]))]): Unit = {
//    geom = getGeometry(inputRDD)
//  }

  override def setX(x: Array[DenseVector[Double]]) = {
    _X = x
    xlength = _X(0).length
    _dX =DenseMatrix.create(rows=xlength,cols=_X.length,data = _X.flatMap(t=>t.toArray))
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

  def specify_res(X: DenseMatrix[Double]= _dX , Y: DenseVector[Double]= _Y , W: DenseMatrix[Double] = DenseMatrix.eye(xlength)): DenseVector[Double] = {
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas0 = xtwx_inv * xtwy
    val y_hat = X * betas0
    Y - y_hat
  }

  def get_env()={
    val wy = spweight_dvec.map(t => (t dot _Y))
//    wy.foreach(println)
    lm_null = specify_res()
    lm_w = specify_res(Y = DenseVector(wy))
//    lm_w.foreach(println)
  }

  //not completed
  def func4optimize(rho: DenseVector[Double]): Unit = {
    get_env()
    val e_a=lm_null * lm_null
    val e_b=lm_w * lm_null
    val e_c=lm_w * lm_w
    val SSE = e_a - rho :*= 2.0 * e_b + rho * rho * e_c

    println(SSE)
  }


}
