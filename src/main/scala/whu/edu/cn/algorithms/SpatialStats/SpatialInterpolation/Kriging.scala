package whu.edu.cn.algorithms.SpatialStats.SpatialInterpolation

import geotrellis.layer.{Bounds, LayoutDefinition, SpaceTimeKey, TileLayerMetadata}
import geotrellis.raster.{MultibandTile, RasterExtent, Tile, TileLayout}
import geotrellis.raster.interpolation.SimpleKrigingMethods
import geotrellis.vector
import geotrellis.vector.interpolation.{NonLinearSemivariogram, Semivariogram, Spherical}
import geotrellis.vector.{Extent, PointFeature}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.locationtech.jts.geom.Geometry
import whu.edu.cn.entity
import scala.math.{max, min}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Kriging {

  /**
   * TODO: fix bug
   * TODO: 简单克里金插值(奇异矩阵报错), modelType参数未使用，是否需要输入掩膜
   *
   * @param featureRDD
   * @param propertyName
   * @param modelType
   * @return
   */
  //块金：nugget，基台：sill，变程：range
  def simpleKriging(implicit sc: SparkContext, featureRDD: RDD[(String, (Geometry, mutable.Map[String, Any]))], propertyName: String, modelType: String) = {
    //Kriging Interpolation for PointsRDD
    val extents = featureRDD.map(t => {
      val coor = t._2._1.getCoordinate
      (coor.x, coor.y, coor.x, coor.y)
    }).reduce((coor1, coor2) => {
      (min(coor1._1, coor2._1), min(coor1._2, coor2._2), max(coor1._3, coor2._3), max(coor1._4, coor2._4))
    })
    val extent = Extent(extents._1, extents._2, extents._3, extents._4)
    val rows = 256
    val cols = 256
    val rasterExtent = RasterExtent(extent, cols, rows)
    val points: Array[PointFeature[Double]] = featureRDD.map(t => {
      val p = vector.Point(t._2._1.getCoordinate)
      //TODO 直接转换为Double类型会报错
      //      var data = t._2._2(propertyName).asInstanceOf[Double]
      var data = t._2._2(propertyName).asInstanceOf[String].toDouble
      if (data < 0) {
        data = 100
      }
      PointFeature(p, data)
    }).collect()
    println()
    val sv: Semivariogram = NonLinearSemivariogram(points, 30000, 0, Spherical)
    val method = new SimpleKrigingMethods {
      //TODO fix bug 简单克里金插值(奇异矩阵报错)
      override def self: Traversable[PointFeature[Double]] = points
    }
    val originCoverage = method.simpleKriging(rasterExtent, sv)


    val tl = TileLayout(10, 10, 256, 256)
    val ld = LayoutDefinition(extent, tl)
    val crs = geotrellis.proj4.CRS.fromEpsgCode(featureRDD.first()._2._1.getSRID)
    val time = System.currentTimeMillis()
    val bounds = Bounds(SpaceTimeKey(0, 0, time), SpaceTimeKey(0, 0, time))
    //TODO cellType的定义
    val cellType = originCoverage.cellType
    val tileLayerMetadata = TileLayerMetadata(cellType, ld, extent, crs, bounds)

    originCoverage.toArrayTile()
    var list: List[Tile] = List.empty
    list = list :+ originCoverage
    val tileRDD = sc.parallelize(list)
    val imageRDD = tileRDD.map(t => {
      val k = entity.SpaceTimeBandKey(SpaceTimeKey(0, 0, time), ListBuffer("interpolation"))
      val v = MultibandTile(t)
      (k, v)
    })

    (imageRDD, tileLayerMetadata)
  }

}
