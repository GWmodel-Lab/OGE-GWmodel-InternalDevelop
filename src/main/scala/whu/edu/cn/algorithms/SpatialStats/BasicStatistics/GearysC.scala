package whu.edu.cn.algorithms.SpatialStats.BasicStatistics

import breeze.linalg.{DenseMatrix, DenseVector, sum, trace, max}
import breeze.stats.mean
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureDistance._
import whu.edu.cn.algorithms.SpatialStats.Utils.FeatureSpatialWeight._

import scala.collection.mutable
import scala.math._

object GearysC {

  private var _data: RDD[mutable.Map[String, Any]] = _
  private var _dmatX: DenseMatrix[Double] = _
  private var _dvecY: DenseVector[Double] = _

  private var _nameX: Array[String] = _
  private var _nameY: String = _
  private var _rows: Int = 0

  protected def setY(property: String): Unit = {
    _nameY = property
    _dvecY = DenseVector(_data.map(t => t(property).asInstanceOf[String].toDouble).collect())
  }

  def globalGearysC(shpRDD: RDD[(String, (Geometry, Map[String, Any]))], y: String, x:String)={

  }

  def localGearysC()={

  }

}
