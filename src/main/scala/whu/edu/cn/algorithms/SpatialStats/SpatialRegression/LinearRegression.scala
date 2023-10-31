package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector, inv}
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry

import scala.collection.mutable

object LinearRegression {

  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _X: DenseMatrix[Double] = _
  private var _Y: DenseVector[Double] = _
  private var _1X: DenseMatrix[Double] = _
  private var _xlength: Int = 0

  private def setX(properties: String, split: String = ",", Intercept: Boolean): Unit = {
    val _nameX = properties.split(split)
    val x = _nameX.map(s => {
      DenseVector(_data.map(t => t(s).asInstanceOf[String].toDouble).collect())
    })
    _xlength = x(0).length
    _X = DenseMatrix.create(rows = _xlength, cols = x.length, data = x.flatMap(t => t.toArray))
    if (Intercept) {
      val ones_x = Array(DenseVector.ones[Double](_xlength).toArray, x.flatMap(t => t.toArray))
      _1X = DenseMatrix.create(rows = _xlength, cols = x.length + 1, data = ones_x.flatten)
    }
  }

  private def setY(property: String): Unit = {
    _Y = DenseVector(_data.map(t => t(property).asInstanceOf[String].toDouble).collect())
  }

  /**
   * 线性回归
   *
   * @param x         输入X
   * @param y         输入Y
   * @param Intercept 是否需要截距项，默认：是（true）
   * @return          （系数，预测值，残差）各自以Array形式储存
   */
  def linearRegression(data: RDD[mutable.Map[String, Any]], y: String, x: String, split: String = ",", Intercept: Boolean =true)
                      : (DenseVector[Double], DenseVector[Double], DenseVector[Double])= {
    _data=data
    setX(x, split, Intercept)
    setY(y)
    var X=_1X
    if(! Intercept){
      X= _X
    }
    val Y=_Y
    val W = DenseMatrix.eye[Double](_xlength)
    val xtw = X.t * W
    val xtwx = xtw * X
    val xtwy = xtw * Y
    val xtwx_inv = inv(xtwx)
    val betas = xtwx_inv * xtwy
    val y_hat = X * betas
    val res = Y - y_hat
    println("success")
    (betas,y_hat,res)
  }

}
