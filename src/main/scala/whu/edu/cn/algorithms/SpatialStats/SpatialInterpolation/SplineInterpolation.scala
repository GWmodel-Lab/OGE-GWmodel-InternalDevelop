package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import breeze.linalg.DenseMatrix
import breeze.numerics.pow
import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster._
import geotrellis.raster.{DoubleCellType, MultibandTile, RasterExtent, Tile, TileLayout, rasterize, render}
import geotrellis.spark.withFeatureRDDRasterizeMethods
import geotrellis.vector
import geotrellis.vector.{Extent, PointFeature}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.algorithms.SpatialStats.Utils.OtherUtils
import whu.edu.cn.entity

import scala.math.{log, max, min}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation.interpolationUtils._
import whu.edu.cn.entity.SpaceTimeBandKey

object SplineInterpolation {

  // thin plate spline
  def fit(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))],
          propertyName: String, rows: Int = 20, cols: Int = 20,
          refinement: Boolean = false,epsilon: Double = 1.0e-4, level: Int = 3)={
    val extent = getExtent(featureRDD)
    val crs = OtherUtils.getCrs(featureRDD)
    val pointsRas = createPredictionPoints(extent, rows, cols)
    //convert points
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      val data = t._2._2(propertyName).asInstanceOf[String].toDouble
      PointFeature(p, data)
    }).collect()


    println("************************************************************")
    println(s"Parameters load correctly, start calculation")

    // linear formula group matrix
    val n = points.length

    val mat_A = DenseMatrix.zeros[Double](n+3,n+3)// [PHI P, P.t 0]
    val mat_b = DenseMatrix.zeros[Double](n+3,1)// [z,0]

    val phi = (r:Double) => pow(r,2)*log(r) //kernel function, r is distance
    for(i <- 0 until n; j <- 0 until n){
      val p_i = points(i).geom
      val p_j = points(j).geom
      val dist = p_i.distance(p_j)
      mat_A(i,j) = phi(dist)
    }// distances
    for(i <- 0 until n){
      val p_i = points(i)
      // coordinates
      mat_A(i,n) = 1
      mat_A(i,n+1) = p_i.getX
      mat_A(i,n+2) = p_i.getY
      mat_A(n,i) = mat_A(i,n)
      mat_A(n+1,i) = mat_A(i,n+1)
      mat_A(n+2,i) = mat_A(i,n+2)
      // attribute values
      mat_b(i,0) = p_i.data
    }

    val mat_a = breeze.linalg.inv(mat_A) * mat_b

    println(mat_a.toDenseVector)



    println("spline interpolation succeeded")
    println("************************************************************")

  }

  private def splineBaseFunction(r: Double, k:Int):Double = {
    if (r == 0) 0
    k match {
      case 1 => math.pow(r,2) * math.log(r)
      case 2 => math.pow(r,3)
      case 3 => math.pow(r,3) * math.log(r)
      case _ => math.pow(r,2) * math.log(r)
    }
  }

}
