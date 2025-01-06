package whu.edu.cn.algorithms.SpatialStats.SpatialRegression

import breeze.linalg.{DenseMatrix, DenseVector}
import org.apache.spark.rdd.RDD
import whu.edu.cn.algorithms.SpatialStats.GWModels.Algorithm

import scala.collection.mutable

object PoissonRegression extends Algorithm {
  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dmatX: DenseMatrix[Double] = _
  private var _dvecY: DenseVector[Double] = _

  private var _rawX: Array[Array[Double]] = _
  private var _rawdX: DenseMatrix[Double] = _

  private var _nameX: Array[String] = _
  private var _nameY: String = _
  private var _rows: Int = 0
  private var _df: Int = 0

  override def setX(properties: String, split: String = ","): Unit = {
    _nameX = properties.split(split)
    val x = _nameX.map(s => {
      _data.map(t => t(s).asInstanceOf[String].toDouble).collect()
    })
    _rows = x(0).length
    _df = x.length

    _rawX = x
    _rawdX = DenseMatrix.create(rows = _rows, cols = x.length, data = x.flatten)
    val onesX = Array(DenseVector.ones[Double](_rows).toArray, x.flatten)
    _dmatX = DenseMatrix.create(rows = _rows, cols = x.length + 1, data = onesX.flatten)
  }

  override def setY(property: String): Unit = {
    _nameY = property
    _dvecY = DenseVector(_data.map(t => t(property).asInstanceOf[String].toDouble).collect())
  }

  def poissonRegression(x: Array[DenseVector[Double]], y: DenseVector[Double]): Unit = {


  }

}
